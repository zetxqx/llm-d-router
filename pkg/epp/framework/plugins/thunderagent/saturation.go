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
	"sort"
	"time"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Saturation refreshes the per-pod fit view, runs the pause sweep, and
// always returns 0.0.
//
// The flow controller's saturation gate is a band-level head-of-line block:
// while it reports saturated, no request dispatches, including the turns of
// programs already admitted. Those turns are what complete trajectories and
// free capacity, so blocking them can only stall the pool. ThunderAgent
// therefore gates in Pick instead, where it can tell an admitted program's
// turn from a paused or new program's turn.
//
// This hook is the plugin's per-cycle feed of the endpoint pool. For each pod
// it computes two views of the working set (upstream ThunderAgent's
// remaining_capacity and remaining_capacity_with_decay):
//
//   - undecayed: every unpaused program at full footprint. When this exceeds
//     the fit ceiling the pause sweep pushes idle programs out, smallest
//     first, and when none are left marks every in-flight program to pause
//     at the end of its turn (upstream _pause_until_safe).
//   - decayed: idle programs decayed with actingHalfLife. The admission room
//     the fairness policy fits paused and new programs into.
//
// The sweep runs per pod at most once per pauseSweep (upstream's scheduler
// interval); the views refresh on every call. Per-stage calls (prefill,
// decode) each maintain their own pods; pods no call reports for 5s age out.
func (a *ThunderAgent) Saturation(ctx context.Context, endpoints []datalayer.Endpoint) float64 {
	now := time.Now()

	type podReport struct {
		pod           string
		util          float64
		capacity      float64
		source        capacitySource
		shared        float64
		paused        int
		marked        int
		tokensAfter   float64
		ceiling       float64
		sweptThisCall bool
	}
	var reports []podReport
	var running, idle, paused, marked int

	t := a.table
	t.mu.Lock()
	loads := t.podLoads(now)
	for _, st := range t.programs {
		switch {
		case st.paused:
			paused++
		case st.markedForPause:
			marked++
		case st.inflightTokens > 0:
			running++
		default:
			idle++
		}
	}

	for _, endpoint := range endpoints {
		md := endpoint.GetMetadata()
		if md == nil {
			continue
		}
		pod := md.ID.String()
		capTokens, source := a.endpointCapacity(endpoint)
		load := loads[pod]
		buffers := a.bufferTokensPerProgram * float64(load.programs)

		// Optional shared_tokens correction: the estimate of the running
		// programs minus what the engine actually holds. Only trusted with
		// real scraped capacity and a real metrics sample; a zero usage
		// float cannot signal absence and would zero out every running
		// program.
		shared := 0.0
		if a.kvUsageCorrection && source == capacityReal {
			if m := endpoint.GetMetrics(); m != nil && !m.UpdateTime.IsZero() {
				if s := load.running - m.KVCacheUsagePercent*capTokens; s > 0 {
					shared = s
				}
			}
		}
		usedUndecayed := load.undecayed - shared + buffers
		usedDecayed := load.decayed - shared + buffers
		ceiling := capTokens * a.utilThreshold

		snap := t.snapshot[pod]
		sweptAt := now
		if snap != nil {
			sweptAt = snap.sweptAt
		}
		report := podReport{pod: pod, capacity: capTokens, source: source, shared: shared, ceiling: ceiling}
		// Interval 0 sweeps on every call including the first; otherwise a pod
		// is first swept one interval after it appears, like upstream's first
		// scheduler tick.
		sweepDue := a.pauseSweep == 0 || (snap != nil && now.Sub(sweptAt) >= a.pauseSweep)
		if capTokens > 0 && sweepDue {
			sweptAt = now
			report.sweptThisCall = true
			if usedUndecayed > ceiling {
				var pausedNow, markedNow int
				usedUndecayed, pausedNow, markedNow = t.pauseFromPodLocked(pod, usedUndecayed, ceiling, a.bufferTokensPerProgram)
				report.paused, report.marked = pausedNow, markedNow
				paused += pausedNow
				marked += markedNow
				idle -= pausedNow
				running -= markedNow
				// Paused programs left the decayed view too; recompute it
				// from the table rather than tracking per-program deltas.
				reloaded := t.podLoads(now)[pod]
				usedDecayed = reloaded.decayed - shared + a.bufferTokensPerProgram*float64(reloaded.programs)
			}
		}
		report.tokensAfter = usedUndecayed

		t.snapshot[pod] = &podSnapshot{
			capacity:  capTokens,
			tokens:    usedUndecayed,
			room:      ceiling - usedDecayed,
			updatedAt: now,
			sweptAt:   sweptAt,
		}

		report.util = 1.0
		if capTokens > 0 {
			report.util = usedUndecayed / capTokens
		}
		reports = append(reports, report)
	}
	for pod, snap := range t.snapshot {
		if now.Sub(snap.updatedAt) > snapshotStaleAfter {
			delete(t.snapshot, pod)
			a.metrics.forgetPod(pod)
		}
	}
	t.mu.Unlock()

	a.metrics.programs.WithLabelValues("running").Set(float64(running))
	a.metrics.programs.WithLabelValues("idle").Set(float64(idle))
	a.metrics.programs.WithLabelValues("marked").Set(float64(marked))
	a.metrics.programs.WithLabelValues("paused").Set(float64(paused))

	logger := log.FromContext(ctx)
	trace := logger.V(logutil.TRACE)
	for _, r := range reports {
		a.metrics.podUtilization.WithLabelValues(r.pod).Set(r.util)
		a.metrics.setPodCapacity(r.pod, r.source, r.capacity)
		if r.paused > 0 || r.marked > 0 {
			a.metrics.pauses.Add(float64(r.paused))
			logger.Info("thunderagent.pause_sweep", "pod", r.pod, "paused", r.paused, "marked", r.marked,
				"working_set", r.tokensAfter, "ceiling", r.ceiling)
		}
		if trace.Enabled() {
			trace.Info("thunderagent fit view", "pod", r.pod, "util", r.util, "capacity", r.capacity,
				"capacitySource", r.source, "shared", r.shared, "swept", r.sweptThisCall)
		}
	}

	return 0.0
}

// pauseFromPodLocked is upstream _pause_until_safe for one pod. While the
// undecayed working set exceeds the ceiling it pauses the pod's idle unpaused
// programs smallest first (no idle-age requirement: upstream pauses any ACTING
// program). When no idle program is left it marks every unmarked in-flight
// program to pause at the end of its turn; upstream's loop condition does not
// account for marks, so it marks them all before it breaks. Returns the
// reduced working set and the counts of programs paused and marked. Paused
// programs keep podName as their origin. Callers must hold t.mu.
func (t *programTable) pauseFromPodLocked(pod string, tokens, ceiling, buffer float64) (float64, int, int) {
	type candidate struct {
		st        *program
		footprint float64
	}
	var idle, running []candidate
	for _, st := range t.programs {
		if st.podName != pod || st.paused {
			continue
		}
		c := candidate{st: st, footprint: footprint(st) + buffer}
		if st.inflightTokens > 0 {
			running = append(running, c)
		} else {
			idle = append(idle, c)
		}
	}
	sort.Slice(idle, func(i, j int) bool { return idle[i].footprint < idle[j].footprint })

	paused := 0
	for _, c := range idle {
		if tokens <= ceiling {
			return tokens, paused, 0
		}
		c.st.paused = true
		c.st.markedForPause = false
		tokens -= c.footprint
		paused++
		t.pausesTotal++
	}
	marked := 0
	if tokens > ceiling {
		for _, c := range running {
			if !c.st.markedForPause {
				c.st.markedForPause = true
				marked++
			}
		}
	}
	return tokens, paused, marked
}
