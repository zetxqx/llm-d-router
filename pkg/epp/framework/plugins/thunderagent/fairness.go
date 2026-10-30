/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package thunderagent

import (
	"context"
	"math"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkfc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionstate"
)

// holdWaitFloor separates admission holds from normal dispatch latency in
// the delayed dispatch metric.
const holdWaitFloor = time.Second

// NewState satisfies the FairnessPolicy contract; Pick reads the shared
// session state instead of per-band state.
func (a *ThunderAgent) NewState(_ context.Context) any { return nil }

// candidate is one dispatchable queue head under consideration.
type candidate struct {
	queue     fwkfc.FlowQueueAccessor
	id        string
	sizeBytes int
	class     sessionClass
	state     sessionstate.SessionState
	// home is the pod the session is bound to, or for a paused session the
	// pod it was paused on.
	home string
	// tokens is the session's size once PreRequest charges this turn.
	tokens   float64
	waitMs   float64
	starving bool
	fitPod   string
}

// Pick is the admission gate for thunder agent.
//
// Turns of admitted sessions always dispatch. A paused session's
// next turn dispatches only when its own pod has room again. A new session dispatches
// only when some pod has room, and is then reserved onto that pod.
//
// Unlike the original ThunderAgent, which restores a paused session onto any
// pod with room, a paused session waits for its own pod, so its warm prefix
// is kept. The wait is bounded by headWaitStarvationMs.
// TODO(#3221): add an option to restore a paused session onto another pod
// with room when its own pod stays full.
//
// The sessions allowed to dispatch are ranked by betterThan.
// A session that fits no pod does not block the others, so smaller sessions
// can take room a larger paused one waits for; the starvation backstop bounds
// that wait.
//
// If no session is allowed (every waiting session is paused or new and fits no
// pod), Pick returns nil. The requests stay queued and the
// processor tries again on its next dispatch cycle.
//
// Room is capacity * utilThreshold minus the working set. When the picked
// session does not fit (including a session force-admitted onto its own pod),
// or an admitted session's turn pushes its pod over the ceiling, sessions idle
// past idleLeaseSeconds give up their room, longest idle first, and are
// paused. Sessions with a turn queued are never paused,
// and nothing is paused until a session is picked.
func (a *ThunderAgent) Pick(ctx context.Context, band fwkfc.PriorityBandAccessor) (fwkfc.FlowQueueAccessor, error) {
	if band == nil {
		return nil, nil //nolint:nilnil
	}
	now := a.clock.Now()

	var candidates []*candidate
	queued := map[string]bool{}
	band.IterateQueues(func(queue fwkfc.FlowQueueAccessor) bool {
		if queue == nil || queue.Len() == 0 {
			return true
		}
		head := queue.Peek()
		if head == nil {
			return true
		}
		id := headSessionID(head)
		waitMs := float64(now.Sub(head.EnqueueTime()).Milliseconds())
		if id != "" {
			queued[id] = true
		}
		candidates = append(candidates, &candidate{
			queue:     queue,
			id:        id,
			sizeBytes: headSizeBytes(head),
			waitMs:    waitMs,
			starving:  a.headWaitStarvationMs > 0 && waitMs >= a.headWaitStarvationMs,
		})
		return true
	})
	// The processor calls Pick every dispatch cycle, queued work or not.
	if len(candidates) == 0 {
		return nil, nil //nolint:nilnil
	}

	// Without the session-state registry the gate fails open and only the
	// ranking remains.
	reg := a.sessions()

	a.mu.Lock()
	resumed := 0
	needsFit := false
	for _, c := range candidates {
		if a.classifyLocked(reg, c, now) {
			resumed++
		}
		needsFit = needsFit || c.class != classAdmitted
	}
	var loads map[string]*podLoad
	if needsFit {
		loads = map[string]*podLoad{}
		for id, capacity := range a.endpointsLocked(reg) {
			load := a.loadLocked(reg, id, capacity, now, queued)
			loads[id] = &load
		}
	}

	var best *candidate
	for _, c := range candidates {
		if !a.fitLocked(c, loads) {
			continue
		}
		if best == nil || betterThan(c, best) {
			best = c
		}
	}
	paused := 0
	if best != nil {
		paused = a.admitLocked(reg, best, now, queued, loads)
	}
	a.mu.Unlock()

	a.metrics.resumes.Add(float64(resumed))
	if paused > 0 {
		a.metrics.pauses.Add(float64(paused))
		log.FromContext(ctx).V(logutil.DEBUG).Info("Paused idle sessions to make room", "sessionClass", best.class.String(), "pausedSessions", paused)
	}
	if best == nil {
		return nil, nil //nolint:nilnil
	}
	a.metrics.releases.WithLabelValues(best.class.String()).Inc()
	if best.waitMs >= float64(holdWaitFloor.Milliseconds()) {
		a.metrics.delayedDispatches.WithLabelValues(best.class.String()).Inc()
	}
	if best.starving {
		a.metrics.starvationPromotions.Inc()
	}
	return best.queue, nil
}

// classifyLocked reads a candidate's session state, ranks it, and sizes its
// turn. Anonymous heads and every head when reg is nil pass as admitted. It
// reports whether the session has resumed from a pause since the last look.
func (a *ThunderAgent) classifyLocked(reg *sessionstate.SessionStateRegistry, c *candidate, now time.Time) bool {
	c.class = classAdmitted
	if c.id == "" || reg == nil {
		return false
	}
	state, tracked := reg.State(c.id)
	resumed := a.refreshLocked(c.id, state, tracked, now)
	c.state = state

	l := a.local[c.id]
	switch {
	case l.pausedLive(state):
		c.class = classNew
		if reg.HasEndpoint(l.pausedEndpoint) {
			c.class, c.home = classPaused, l.pausedEndpoint
		}
	case !tracked || state.TurnsTaken == 0:
		c.class = classNew
	case state.Status == sessionstate.StatusArchived:
		// An archived session counts nothing on its pod, so its next turn
		// must fit there again, like a paused one.
		c.class, c.home = classPaused, state.BoundEndpoint
	case state.BoundEndpoint != "":
		c.home = state.BoundEndpoint
	case reg.HasEndpoint(state.LastBoundEndpoint):
		// The registry evicted the session's blocks but its pod is still in
		// the pool: the session stays admitted and returns to that pod.
		c.home = state.LastBoundEndpoint
	default:
		// Its pod left the pool.
		c.class = classNew
	}

	// The new turn resends the whole history, so its estimate is the
	// session's size from now on; committed tokens are the floor. PreRequest
	// adds the estimate to the turns already in flight.
	c.tokens = max(float64(state.CommittedTokens), float64(state.InFlightTokens)+float64(max(estimateTokens(c.sizeBytes), 1)))
	return resumed
}

// fitLocked finds, for a paused or new session, the pod it fits once
// reclaimable idle sessions give up their room. Returns false when the
// candidate must hold.
func (a *ThunderAgent) fitLocked(c *candidate, loads map[string]*podLoad) bool {
	if c.class == classAdmitted || len(loads) == 0 {
		return true
	}
	switch c.class {
	case classPaused:
		if p := loads[c.home]; p != nil && p.room(a.utilThreshold)+p.spare >= a.fitTokens(p, c.tokens) {
			c.fitPod = c.home
		}
	case classNew:
		c.fitPod = a.newSessionPod(c.tokens, loads)
	}
	return c.fitPod != "" || c.starving
}

// fitTokens is the room a session needs on a pod: its size, capped at the
// pod's ceiling. A session larger than the ceiling (it grew there through
// turns that skip the fit check) then fits once every other session on the
// pod is paused or reclaimable, instead of never.
func (a *ThunderAgent) fitTokens(p *podLoad, tokens float64) float64 {
	return min(tokens, p.capacity*a.utilThreshold)
}

// newSessionPod picks the pod for a new session: among pods it fits once
// reclaimable idle sessions give up their room, the one with the most free
// room. If any pod fits it without pausing anyone, that pod has the most
// free room, so no one is paused.
func (a *ThunderAgent) newSessionPod(tokens float64, loads map[string]*podLoad) string {
	var best *podLoad
	for _, p := range loads {
		room := p.room(a.utilThreshold)
		if room+p.spare >= a.fitTokens(p, tokens) && (best == nil || room > best.room(a.utilThreshold)) {
			best = p
		}
	}
	if best == nil {
		return ""
	}
	return best.id
}

// admitLocked commits the picked session. It pauses idle sessions on the
// session's pod until the turn fits, then reserves the turn's room until
// PreRequest charges it. The reservation keeps the next cycles from admitting
// into the same room twice, keeps a dispatched admitted session, no longer
// queued and not yet in flight, from looking idle and being reclaimed, and
// pins the session to the reserved pod in Score. Returns how many sessions it
// paused.
func (a *ThunderAgent) admitLocked(reg *sessionstate.SessionStateRegistry, best *candidate, now time.Time,
	queued map[string]bool, loads map[string]*podLoad) int {
	if best.id == "" || reg == nil {
		return 0
	}
	var target string
	switch {
	case best.class == classAdmitted:
		target = best.home
	case best.fitPod != "":
		target = best.fitPod
	case best.home != "":
		// A force-admitted session that fits no pod returns to its own pod.
		target = best.home
	default:
		// A force-admitted new session that fits no pod: the scheduler
		// places it and PreRequest binds it.
		return 0
	}

	load, ok := loads[target]
	if !ok {
		capacity, known := a.capacityLocked(reg, target)
		if !known {
			return 0
		}
		computed := a.loadLocked(reg, target, capacity, now, queued)
		load = &computed
	}
	need := a.fitTokens(load, best.tokens)
	if best.class == classAdmitted {
		// Its current size is already counted; only the turn's growth is new.
		need = max(best.tokens-size(best.state), 0)
	}
	paused := a.reclaimLocked(*load, load.room(a.utilThreshold), need)

	l := a.localLocked(best.id)
	l.reservedEndpoint = target
	l.reservedTokens = best.tokens
	l.reservedUntil = now.Add(reservationTTL)
	l.reservedAtTurns = best.state.TurnsTaken
	return paused
}

// betterThan reports whether a outranks b:
// waited past headWaitStarvationMs (oldest first) ->
// class (admitted -> paused -> new) -> smallest footprint -> oldest.
func betterThan(a, b *candidate) bool {
	if a.starving != b.starving {
		return a.starving
	}
	if a.starving {
		return a.waitMs > b.waitMs
	}
	if a.class != b.class {
		return a.class < b.class
	}
	if a.tokens != b.tokens {
		return a.tokens < b.tokens
	}
	return a.waitMs > b.waitMs
}

// headSessionID returns the session of a queue head, read the same way as in
// the scorer, or "" for anonymous traffic. A queue is named by the fairness
// ID, which can group several sessions.
func headSessionID(head fwkfc.QueueItemAccessor) string {
	req := head.OriginalRequest()
	if req == nil {
		return ""
	}
	return sessionID(req.InferenceRequest())
}

// headSizeBytes returns the request body size of a queue head, from the
// scheduling request when the item carries one (the production adapter
// does), else from the flow-control item's byte size.
func headSizeBytes(head fwkfc.QueueItemAccessor) int {
	req := head.OriginalRequest()
	if req == nil {
		return 0
	}
	if ir := req.InferenceRequest(); ir != nil && ir.RequestSizeBytes > 0 {
		return ir.RequestSizeBytes
	}
	if n := req.ByteSize(); n <= math.MaxInt {
		return int(n)
	}
	return math.MaxInt
}
