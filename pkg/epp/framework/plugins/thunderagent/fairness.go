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
)

// programClass ranks a waiting program for dispatch. Lower dispatches first.
type programClass int

const (
	// classReasoning is an admitted, unpaused program with a request waiting.
	// Its footprint is already counted in the working set and finishing its
	// trajectory is what returns capacity to the pool, so it always
	// dispatches (upstream: an ACTIVE program's request is proxied
	// unchecked).
	classReasoning programClass = iota
	// classPaused is a paused program whose next turn is waiting. It must
	// fit a pod again (its footprint stopped counting when it was paused),
	// but it outranks never-admitted programs (upstream _greedy_resume puts
	// REASONING programs with step > 1 first): it is mid-trajectory with
	// sunk work, and finishing it releases capacity permanently.
	classPaused
	// classNew is a program whose first request is waiting. It is admitted
	// only when its projected footprint fits a pod under the utilThreshold
	// ceiling.
	classNew
)

func (c programClass) String() string {
	switch c {
	case classReasoning:
		return "reasoning"
	case classPaused:
		return "paused"
	default:
		return "new"
	}
}

// holdWaitFloor separates admission holds from normal dispatch latency in
// the holds metric: a request that waited at least this long in the
// flow-control queue counts as held.
const holdWaitFloor = time.Second

// Pick selects the queue to service next.
//
// Turns of admitted (REASONING) programs bypass admission: smallest footprint
// first, then the oldest head. Paused programs are considered next, then new
// programs, smallest footprint first within each class, and each must fit a
// pod: decayed working set plus live reservations plus the program's size and
// growth buffer, within utilThreshold of the pod's capacity. A paused program
// is sized at the larger of its committed tokens and the new turn's estimate
// (upstream re-estimates before the program waits) and prefers its origin
// pod when that fits; with resumePlacement origin-only it waits for the
// origin instead of moving (until originWaitMaxMs, if set). A fitting program gets a reservation charged to the pod
// that fit it, released when PreRequest binds the program. A paused or new
// head waiting past urgentWaitMs is urgent: it outranks every non-urgent
// paused or new program (oldest first); with urgentMove it may also take any
// pod with room under origin-only; with urgentReserveOrigin the pod it waits
// for admits no other paused or new program until it fits. It still needs
// room. A head waiting past headWaitStarvationMs dispatches regardless of
// class, size, or fit, oldest first: the forced-admission backstop.
//
// Returning nil while only non-fitting paused or new programs wait is the
// hold: the processor dispatches nothing and retries next cycle.
//
// The fit view comes from the Saturation hook, so the plugin must be the
// flow control saturation detector. Without a fit view, programs are
// admitted freely and only the class ordering remains.
//
// There is no ACTING class. Upstream ThunderAgent's resume pool can hold a
// paused program with no request outstanding; flow control only ever queues
// actual requests, so every candidate here has a request pending.
func (a *ThunderAgent) Pick(ctx context.Context, band fwkfc.PriorityBandAccessor) (fwkfc.FlowQueueAccessor, error) {
	if band == nil {
		return nil, nil //nolint:nilnil
	}

	now := time.Now()
	logger := log.FromContext(ctx)

	type candidate struct {
		queue    fwkfc.FlowQueueAccessor
		class    programClass
		tokens   float64
		waitMs   float64
		starving bool
		urgent   bool
		fitPod   string
	}

	var best *candidate
	held := 0

	t := a.table
	t.mu.Lock()
	pendingByPod := t.pendingByPod(now)
	// Pods reserved for urgent paused programs that do not fit their origin
	// yet: closed to every other paused or new admission this cycle.
	reserved := map[string]bool{}
	if a.urgentReserveOrigin && a.urgentWaitMs > 0 {
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
			class, tokens := t.classAndTokens(id)
			if class != classPaused || waitMs < a.urgentWaitMs || (a.headWaitStarvationMs > 0 && waitMs >= a.headWaitStarvationMs) {
				return true
			}
			if est := float64(t.estimateTokens(headSizeBytes(head))); est > tokens {
				tokens = est
			}
			origin := t.programs[id].podName
			if snap, ok := t.snapshot[origin]; ok && snap.room-pendingByPod[origin] < tokens+a.bufferTokensPerProgram {
				reserved[origin] = true
			}
			return true
		})
	}
	t.reservedPods = len(reserved)
	band.IterateQueues(func(queue fwkfc.FlowQueueAccessor) bool {
		// Empty entries appear transiently when a queue drains between
		// iteration and scoring; they carry nothing to service.
		if queue == nil || queue.Len() == 0 {
			return true
		}
		head := queue.Peek()
		if head == nil {
			return true
		}
		id := queue.FlowKey().ID
		waitMs := float64(now.Sub(head.EnqueueTime()).Milliseconds())
		class, tokens := t.classAndTokens(id)
		starving := a.headWaitStarvationMs > 0 && waitMs >= a.headWaitStarvationMs
		urgent := class != classReasoning && !starving && a.urgentWaitMs > 0 && waitMs >= a.urgentWaitMs

		fitPod := ""
		if class != classReasoning {
			// The new turn resends the whole history, so its estimate is
			// the program's size from now on; a paused program's committed
			// tokens are the floor in case the estimate runs low.
			if est := float64(t.estimateTokens(headSizeBytes(head))); est > tokens {
				tokens = est
			}
			preferred := ""
			if class == classPaused {
				preferred = t.programs[id].podName
			}
			required := tokens + a.bufferTokensPerProgram
			// The origin-only restriction lifts for an urgent program with urgentMove,
			// and for any paused program that has waited past originWaitMaxMs.
			waitForOrigin := a.resumeOriginOnly && !(urgent && a.urgentMove) && !(a.originWaitMaxMs > 0 && waitMs >= a.originWaitMaxMs)
			// A reserved pod is open only to the urgent programs waiting for it.
			var blocked map[string]bool
			if len(reserved) > 0 && !(urgent && reserved[preferred]) {
				blocked = reserved
			}
			pod, ok := a.fitPodLocked(required, preferred, pendingByPod, waitForOrigin, blocked)
			if !ok && !starving {
				held++
				if waitForOrigin && preferred != "" {
					// The hold is the policy's doing only if another pod
					// would have taken the program; record it for the
					// origin-waits counter at resume time.
					if _, elsewhere := a.mostRoomLocked(required, pendingByPod, nil); elsewhere {
						t.programs[id].originHeld = true
					}
				}
				return true
			}
			fitPod = pod
		}

		c := &candidate{queue: queue, class: class, tokens: tokens, waitMs: waitMs, starving: starving, urgent: urgent, fitPod: fitPod}
		if best == nil || betterThan(c.class, c.tokens, c.waitMs, c.starving, c.urgent,
			best.class, best.tokens, best.waitMs, best.starving, best.urgent) {
			best = c
		}
		return true
	})

	if best != nil && best.class != classReasoning && best.fitPod != "" {
		// Reserve the fitting pod's room until PreRequest binds the program.
		t.pending[best.queue.FlowKey().ID] = pendingAdmission{
			tokens: best.tokens + a.bufferTokensPerProgram, pod: best.fitPod, at: now,
		}
	}
	if best != nil && best.urgent {
		t.urgentPromotionsTotal++
	}
	t.mu.Unlock()
	a.metrics.reservedPods.Set(float64(len(reserved)))

	if best == nil {
		if held > 0 {
			logger.V(logutil.DEBUG).Info("thunderagent.hold", "held", held)
		}
		return nil, nil //nolint:nilnil
	}

	a.metrics.releases.WithLabelValues(best.class.String()).Inc()
	wasHeld := best.waitMs >= float64(holdWaitFloor.Milliseconds())
	if wasHeld {
		a.metrics.holds.WithLabelValues(best.class.String()).Inc()
	}
	if best.starving {
		a.metrics.starvationPromotions.Inc()
	}
	if best.urgent {
		a.metrics.urgentPromotions.Inc()
	}
	if wasHeld || best.starving || best.urgent {
		logger.V(logutil.DEBUG).Info("thunderagent.release",
			"class", best.class.String(), "waited_ms", best.waitMs, "starved", best.starving, "urgent", best.urgent)
	}
	return best.queue, nil
}

// headSizeBytes returns the request body size of a queue head, from the
// scheduling request when the item carries one (the production adapter
// does), else from the flow control item's byte size.
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

// pendingByPod sums live reservations per pod, dropping expired ones.
// Callers must hold t.mu.
func (t *programTable) pendingByPod(now time.Time) map[string]float64 {
	byPod := make(map[string]float64, len(t.snapshot))
	for id, p := range t.pending {
		if now.Sub(p.at) > pendingAdmissionTTL {
			delete(t.pending, id)
			continue
		}
		byPod[p.pod] += p.tokens
	}
	return byPod
}

// fitPodLocked returns the pod to admit a program of the required size onto,
// if any. Room is the decayed admission room from the fit view minus live
// reservations. The preferred pod (a paused program's origin, where its
// prefix is warm) wins whenever it fits and is not blocked. When it does not
// fit, the pod with the most room is chosen, unless waitForOrigin is set
// (origin-only placement and the program may not move) and the preferred pod
// is still in the fit view: then the program waits for it. Blocked pods
// (reserved for urgent waiters) are never chosen. A preferred pod missing
// from the fit view (left the pool, or stale) is placed by room under either
// policy. With no fit view the check fails open: the plugin is then not wired
// as the saturation detector and only class ordering applies. Callers must
// hold t.mu.
func (a *ThunderAgent) fitPodLocked(required float64, preferred string, pendingByPod map[string]float64, waitForOrigin bool, blocked map[string]bool) (string, bool) {
	t := a.table
	if len(t.snapshot) == 0 {
		return "", true
	}
	if snap, ok := t.snapshot[preferred]; ok && preferred != "" {
		if snap.room-pendingByPod[preferred] >= required && !blocked[preferred] {
			return preferred, true
		}
		if waitForOrigin {
			return "", false
		}
	}
	return a.mostRoomLocked(required, pendingByPod, blocked)
}

// mostRoomLocked returns the unblocked pod with the most room net of
// reservations that covers the required size, if any. Callers must hold t.mu.
func (a *ThunderAgent) mostRoomLocked(required float64, pendingByPod map[string]float64, blocked map[string]bool) (string, bool) {
	bestPod := ""
	bestRoom := 0.0
	for pod, snap := range a.table.snapshot {
		if blocked[pod] {
			continue
		}
		room := snap.room - pendingByPod[pod]
		if room >= required && room > bestRoom {
			bestPod, bestRoom = pod, room
		}
	}
	return bestPod, bestPod != ""
}

// betterThan reports whether candidate a outranks the incumbent b: starving
// first (oldest first), then urgent (oldest first), then class, then the
// smaller footprint, then the older head.
func betterThan(aClass programClass, aTokens, aWait float64, aStarving, aUrgent bool,
	bClass programClass, bTokens, bWait float64, bStarving, bUrgent bool) bool {
	if aStarving != bStarving {
		return aStarving
	}
	if aStarving && bStarving {
		return aWait > bWait
	}
	if aUrgent != bUrgent {
		return aUrgent
	}
	if aUrgent && bUrgent {
		return aWait > bWait
	}
	if aClass != bClass {
		return aClass < bClass
	}
	if aTokens != bTokens {
		return aTokens < bTokens
	}
	return aWait > bWait
}
