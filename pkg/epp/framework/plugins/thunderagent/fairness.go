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
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkfc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/metadata"
)

// holdWaitFloor separates admission holds from normal dispatch latency in
// the holds metric.
const holdWaitFloor = time.Second

// NewState satisfies the FairnessPolicy contract; Pick reads the shared
// sessionManager instead of per-band state.
func (a *ThunderAgent) NewState(_ context.Context) any { return nil }

// candidate is one dispatchable queue head under consideration.
type candidate struct {
	Candidate
	queue     fwkfc.FlowQueueAccessor
	sizeBytes int
	starving  bool
	fitPod    *endpointState
}

// podView is one pod as a single Pick sees it: its free room, its idle
// sessions that can give up their room (with the predictor's estimate) and
// their sizes by id, and the room the policy would let each class reclaim.
type podView struct {
	room   float64
	idle   []SessionInfo
	tokens map[string]float64
	spare  map[SessionClass]float64
}

// Pick is the admission gate for thunder agent.
//
// Turns of admitted sessions always dispatch. A paused session's
// next turn dispatches only when its own pod has room again. A new session dispatches
// only when some pod has room, and is then reserved onto that pod.
//
// Order among the sessions allowed to dispatch: waited past
// headWaitStarvationMs (oldest first), then the AdmissionPolicy's order (by
// default class (admitted -> paused -> new) -> smallest footprint -> oldest).
// A session that fits no pod does not block the others, so smaller sessions
// can take room a larger paused one waits for; the starvation backstop bounds
// that wait.
//
// If no session is allowed (every waiting session is paused or new and fits no
// pod), Pick returns nil. The requests stay queued and the
// processor tries again on its next dispatch cycle.
//
// Room is capacity * utilThreshold minus the working set. When the picked
// session does not fit, idle sessions the AdmissionPolicy offers give up
// their room, in its order, and are paused. Sessions with a turn in flight or
// queued are never paused, and nothing is paused until a session is picked.
func (a *ThunderAgent) Pick(ctx context.Context, band fwkfc.PriorityBandAccessor) (fwkfc.FlowQueueAccessor, error) {
	if band == nil {
		return nil, nil //nolint:nilnil
	}
	now := time.Now()

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
		id := queue.FlowKey().ID
		wait := now.Sub(head.EnqueueTime())
		queued[id] = true
		candidates = append(candidates, &candidate{
			Candidate: Candidate{SessionID: id, Wait: wait},
			queue:     queue,
			sizeBytes: headSizeBytes(head),
			starving:  a.headWaitStarvationMs > 0 && float64(wait.Milliseconds()) >= a.headWaitStarvationMs,
		})
		return true
	})
	// The processor calls Pick every dispatch cycle, queued work or not.
	if len(candidates) == 0 {
		return nil, nil //nolint:nilnil
	}

	m := a.mgr
	m.mu.Lock()
	views := make(map[*endpointState]*podView, len(m.endpoints))
	for _, p := range m.endpoints {
		v := &podView{
			room:   p.capacity*a.utilThreshold - p.occupancy(now),
			idle:   a.idleInfosLocked(p, now, queued),
			tokens: map[string]float64{},
			spare:  map[SessionClass]float64{},
		}
		for _, s := range v.idle {
			v.tokens[s.ID] = s.Tokens
		}
		views[p] = v
	}

	var best *candidate
	held := 0
	for _, c := range candidates {
		if !a.sizeAndFitLocked(c, views) {
			held++
			continue
		}
		if best == nil || a.betterThan(c, best) {
			best = c
		}
	}
	paused := 0
	if best != nil {
		paused = a.admitLocked(best, now, views)
	}
	m.mu.Unlock()

	if paused > 0 {
		a.metrics.pauses.Add(float64(paused))
		log.FromContext(ctx).V(logutil.DEBUG).Info("thunderagent.reclaim", "class", best.Class.String(), "paused", paused)
	}
	if best == nil {
		if held > 0 {
			log.FromContext(ctx).V(logutil.DEBUG).Info("thunderagent.hold", "held", held)
		}
		return nil, nil //nolint:nilnil
	}
	a.metrics.releases.WithLabelValues(best.Class.String()).Inc()
	if best.Wait >= holdWaitFloor {
		a.metrics.holds.WithLabelValues(best.Class.String()).Inc()
	}
	if best.starving {
		a.metrics.starvationPromotions.Inc()
	}
	return best.queue, nil
}

// idleInfosLocked returns the pod's sessions that can give up their room, as
// the policies see them.
func (a *ThunderAgent) idleInfosLocked(p *endpointState, now time.Time, queued map[string]bool) []SessionInfo {
	idle := p.idleSessions(now, queued)
	infos := make([]SessionInfo, 0, len(idle))
	for id, s := range idle {
		idleFor := now.Sub(s.lastResponseAt)
		infos = append(infos, SessionInfo{
			ID:             id,
			Tokens:         s.size(),
			TurnCount:      s.turnCount,
			LastResponseAt: s.lastResponseAt,
			IdleFor:        idleFor,
			NextTurnIn:     a.predictor.NextTurnIn(id, idleFor),
		})
	}
	return infos
}

// spareLocked is the room the policy would let a session of class forClass
// reclaim on the pod. Only the pod's idle sessions count, each once, at the
// ledger's size, whatever the policy returns.
func (a *ThunderAgent) spareLocked(v *podView, forClass SessionClass) float64 {
	if spare, ok := v.spare[forClass]; ok {
		return spare
	}
	var spare float64
	counted := map[string]bool{}
	for _, s := range a.policy.Victims(forClass, v.idle) {
		if tokens, ok := v.tokens[s.ID]; ok && !counted[s.ID] {
			spare += tokens
			counted[s.ID] = true
		}
	}
	v.spare[forClass] = spare
	return spare
}

// sizeAndFitLocked classifies and sizes a candidate and, for a paused or new
// session, finds the pod it fits once the idle sessions the policy offers give
// up their room. Returns false when the candidate must hold.
func (a *ThunderAgent) sizeAndFitLocked(c *candidate, views map[*endpointState]*podView) bool {
	s := a.mgr.sessions[c.SessionID]
	var committed float64
	switch {
	case c.SessionID == metadata.DefaultFairnessID:
		// Anonymous traffic is not tracked and passes through.
		c.Class = ClassAdmitted
		return true
	case s == nil:
		c.Class = ClassNew
	default:
		c.Class = s.class()
		committed = float64(s.committedTokens)
	}

	// The new turn resends the whole history, so its estimate is the
	// session's size from now on; committed tokens are the floor.
	c.Tokens = max(committed, float64(estimateTokens(c.sizeBytes)))
	if c.Class == ClassAdmitted || len(views) == 0 {
		return true
	}
	switch c.Class {
	case ClassPaused:
		v := views[s.endpoint]
		if v.room+a.spareLocked(v, ClassPaused) >= a.fitTokens(s.endpoint, c.Tokens) {
			c.fitPod = s.endpoint
		}
	case ClassNew:
		c.fitPod = a.newSessionPod(c.Tokens, views)
	}
	return c.fitPod != nil || c.starving
}

// fitTokens is the room a session needs on a pod: its size, capped at the
// pod's ceiling. A session larger than the ceiling (it grew there through
// turns that skip the fit check) then fits once every other session on the
// pod is paused or reclaimable, instead of never.
func (a *ThunderAgent) fitTokens(p *endpointState, tokens float64) float64 {
	return min(tokens, p.capacity*a.utilThreshold)
}

// newSessionPod picks the pod for a new session: among pods it fits once
// reclaimable idle sessions give up their room, the one with the most free
// room. If any pod fits it without pausing anyone, that pod has the most
// free room, so no one is paused.
func (a *ThunderAgent) newSessionPod(tokens float64, views map[*endpointState]*podView) *endpointState {
	var best *endpointState
	for p, v := range views {
		if v.room+a.spareLocked(v, ClassNew) >= a.fitTokens(p, tokens) && (best == nil || v.room > views[best].room) {
			best = p
		}
	}
	return best
}

// admitLocked commits the picked session. It pauses idle sessions on the
// session's pod until the turn fits, then reserves the turn's room until
// PreRequest charges it. The reservation keeps the next cycles from admitting
// into the same room twice, and keeps a dispatched admitted session, no
// longer queued and not yet in flight, from looking idle and being reclaimed.
// Returns how many sessions it paused.
func (a *ThunderAgent) admitLocked(best *candidate, now time.Time, views map[*endpointState]*podView) int {
	m := a.mgr
	s := m.sessions[best.SessionID]
	paused := 0
	switch {
	case best.Class == ClassAdmitted:
		if s == nil {
			return 0 // anonymous traffic
		}
		// Its current size is already counted; only the turn's growth is new.
		paused = a.reclaimLocked(s.endpoint, views[s.endpoint], ClassAdmitted, max(best.Tokens-s.size(), 0), now)
	case best.fitPod != nil:
		paused = a.reclaimLocked(best.fitPod, views[best.fitPod], best.Class, a.fitTokens(best.fitPod, best.Tokens), now)
		s = m.bindLocked(best.SessionID, best.fitPod)
	case s == nil:
		// A force-admitted new session that fits no pod: the scheduler
		// places it and PreRequest binds it.
		return 0
	}
	s.reservedTokens = best.Tokens
	s.reservedUntil = now.Add(reservationTTL)
	return paused
}

// reclaimLocked pauses the idle sessions the policy offers for forClass, in
// its order, until the pod's room covers need, and returns how many it
// paused. A victim that is not one of the pod's idle sessions (unknown, turn
// in flight or queued, live reservation, already paused) is skipped.
func (a *ThunderAgent) reclaimLocked(p *endpointState, v *podView, forClass SessionClass, need float64, now time.Time) int {
	room := v.room
	if room >= need {
		return 0
	}
	paused := 0
	for _, info := range a.policy.Victims(forClass, v.idle) {
		if room >= need {
			break
		}
		s := p.sessions[info.ID]
		if _, idle := v.tokens[info.ID]; !idle || s == nil || !s.pausable(now) {
			continue
		}
		s.paused = true
		room += s.size()
		paused++
	}
	return paused
}

// betterThan reports whether a outranks b: heads past headWaitStarvationMs
// first (oldest first), then the AdmissionPolicy's order.
func (a *ThunderAgent) betterThan(x, y *candidate) bool {
	if x.starving != y.starving {
		return x.starving
	}
	if x.starving {
		return x.Wait > y.Wait
	}
	return a.policy.Less(&x.Candidate, &y.Candidate)
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
	return int(req.ByteSize())
}
