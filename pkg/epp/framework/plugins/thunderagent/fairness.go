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
// ledger instead of per-band state.
func (a *ThunderAgent) NewState(_ context.Context) any { return nil }

// candidate is one dispatchable queue head under consideration.
type candidate struct {
	queue     fwkfc.FlowQueueAccessor
	id        string
	sizeBytes int
	class     sessionClass
	tokens    float64
	// growth is how much a reasoning turn raises its session's footprint
	// once PreRequest charges it.
	growth   float64
	waitMs   float64
	starving bool
	fitPod   *endpointState
}

// Pick is the admission gate.
//
// Turns of admitted (reasoning) sessions always dispatch. A paused session's
// next turn dispatches only when its own pod has room (strict origin
// affinity: the session never moves). A new session dispatches only when
// some pod has room, and is then reserved onto that pod. Ordering among the
// heads that may dispatch: force-admitted heads first (oldest first), then
// reasoning before paused before new, then the smallest footprint, then the
// oldest head. A head that does not fit does not block others, so smaller
// sessions can take room a larger paused one waits for; the starvation
// backstop bounds that wait. Returning nil while only non-fitting sessions
// wait is the hold; the processor retries next cycle.
//
// Room is capacity * utilThreshold minus the pod's working set. Idle
// sessions give up room on demand, longest idle first, and are paused: to
// admit a paused or new session only those idle past idleLeaseSeconds, and
// to cover a reasoning turn that pushes its pod over the ceiling any idle
// session (upstream _pause_until_safe pauses any ACTING program). Sessions
// with a turn queued in the band are never reclaimed. Nothing is paused
// until a head is picked, so a head that is evaluated but not picked costs
// nothing.
//
// With no pods in the ledger the check fails open (the plugin is not wired
// as the saturation detector, or no pod reports its KV capacity) and only
// the class ordering remains.
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
		waitMs := float64(now.Sub(head.EnqueueTime()).Milliseconds())
		queued[id] = true
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

	m := a.mgr
	m.mu.Lock()
	rooms := make(map[*endpointState]float64, len(m.endpoints))
	spare := make(map[*endpointState]float64, len(m.endpoints))
	for _, p := range m.endpoints {
		rooms[p] = p.capacity*a.utilThreshold - p.occupancy(now)
		spare[p] = p.reclaimableTokens(now, a.idleLease, queued)
	}

	var best *candidate
	held := 0
	for _, c := range candidates {
		if !a.sizeAndFitLocked(c, rooms, spare) {
			held++
			continue
		}
		if best == nil || betterThan(c, best) {
			best = c
		}
	}
	paused := 0
	if best != nil {
		paused = a.admitLocked(best, now, rooms, queued)
	}
	m.mu.Unlock()

	if paused > 0 {
		a.metrics.pauses.Add(float64(paused))
		log.FromContext(ctx).V(logutil.DEBUG).Info("thunderagent.reclaim", "class", best.class.String(), "paused", paused)
	}
	if best == nil {
		if held > 0 {
			log.FromContext(ctx).V(logutil.DEBUG).Info("thunderagent.hold", "held", held)
		}
		return nil, nil //nolint:nilnil
	}
	a.metrics.releases.WithLabelValues(best.class.String()).Inc()
	if best.waitMs >= float64(holdWaitFloor.Milliseconds()) {
		a.metrics.holds.WithLabelValues(best.class.String()).Inc()
	}
	if best.starving {
		a.metrics.starvationPromotions.Inc()
	}
	return best.queue, nil
}

// sizeAndFitLocked classifies and sizes a candidate and, for a paused or new
// session, finds the pod it fits once reclaimable idle sessions give up
// their room. Returns false when the candidate must hold.
func (a *ThunderAgent) sizeAndFitLocked(c *candidate, rooms, spare map[*endpointState]float64) bool {
	m := a.mgr
	s := m.sessions[c.id]
	switch {
	case c.id == metadata.DefaultFairnessID:
		// Anonymous traffic is not tracked and passes through.
		c.class = classReasoning
		return true
	case s == nil:
		c.class = classNew
	default:
		c.class = s.class()
		if c.class == classReasoning {
			c.tokens = s.size()
		} else {
			c.tokens = float64(s.committedTokens)
		}
	}

	// The new turn resends the whole history, so its estimate is the
	// session's size from now on; committed tokens are the floor.
	est := float64(m.estimateTokensLocked(c.sizeBytes))
	if c.class == classReasoning {
		// PreRequest adds the estimate to the turns already in flight.
		c.growth = max(float64(s.inflightTokens)+est, float64(s.committedTokens)) - c.tokens
		return true
	}
	c.tokens = max(c.tokens, est)
	if len(rooms) == 0 {
		return true
	}
	switch c.class {
	case classPaused:
		if rooms[s.endpoint]+spare[s.endpoint] >= a.fitTokens(s.endpoint, c.tokens) {
			c.fitPod = s.endpoint
		}
	case classNew:
		c.fitPod = a.newSessionPod(c.tokens, rooms, spare)
	}
	return c.fitPod != nil || c.starving
}

// fitTokens is the room a head needs on a pod: its size, capped at the pod's
// ceiling. A session larger than the ceiling (it grew there through turns
// that skip the fit check) then fits once every other session on the pod is
// paused or reclaimable, instead of never.
func (a *ThunderAgent) fitTokens(p *endpointState, tokens float64) float64 {
	return min(tokens, p.capacity*a.utilThreshold)
}

// newSessionPod picks the pod for a new session: among pods it fits once
// reclaimable idle sessions give up their room, the one with the most free
// room. If any pod fits it without pausing anyone, that pod has the most
// free room, so no one is paused.
func (a *ThunderAgent) newSessionPod(tokens float64, rooms, spare map[*endpointState]float64) *endpointState {
	var best *endpointState
	for p, room := range rooms {
		if room+spare[p] >= a.fitTokens(p, tokens) && (best == nil || room > rooms[best]) {
			best = p
		}
	}
	return best
}

// admitLocked commits the picked head. It pauses idle sessions on the head's
// pod until the turn fits, then reserves the turn's room until PreRequest
// charges it. The reservation keeps the next cycles from admitting into the
// same room twice, and keeps a dispatched reasoning session, no longer
// queued and not yet in flight, from looking idle and being reclaimed.
// Returns how many sessions it paused.
func (a *ThunderAgent) admitLocked(best *candidate, now time.Time, rooms map[*endpointState]float64, queued map[string]bool) int {
	m := a.mgr
	s := m.sessions[best.id]
	reserve := best.tokens
	paused := 0
	switch {
	case best.class == classReasoning:
		if s == nil {
			return 0 // anonymous traffic
		}
		paused = s.endpoint.reclaim(now, 0, queued, rooms[s.endpoint], best.growth)
		reserve += best.growth
	case best.fitPod != nil:
		paused = best.fitPod.reclaim(now, a.idleLease, queued, rooms[best.fitPod], a.fitTokens(best.fitPod, best.tokens))
		s = m.bindLocked(best.id, best.fitPod)
	case s == nil:
		// A force-admitted new session that fits no pod: the scheduler
		// places it and PreRequest binds it.
		return 0
	}
	s.reservedTokens = reserve
	s.reservedUntil = now.Add(reservationTTL)
	return paused
}

// betterThan reports whether a outranks b: force-admitted first (oldest
// first), then class, then the smaller footprint, then the older head.
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
