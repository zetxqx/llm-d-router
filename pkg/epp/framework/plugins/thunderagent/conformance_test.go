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

// Conformance tests against upstream ThunderAgent's tr-decay semantics
// (ThunderAgent/scheduler/router.py, backend/state.py): which view decides
// what, what the pause sweep touches, how marks and reservations behave at
// the edges, and an accounting invariant check under a random workload.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func programsGauge(a *ThunderAgent, state string) float64 {
	return testutil.ToFloat64(a.metrics.programs.WithLabelValues(state))
}

// Upstream pauses on remaining_capacity (undecayed) and resumes on
// remaining_capacity_with_decay: an idle program that has decayed to nothing
// in the admission view is still paused by the sweep.
func TestSweepUsesUndecayedViewEvenWhenDecayedViewHasRoom(t *testing.T) {
	cfg := testConfig()
	cfg.ActingHalfLifeSeconds = 1
	a := newTestAgent(cfg)
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "stale", sched[0], 1200)
	a.table.mu.Lock()
	a.table.programs["stale"].lastResponseAt = time.Now().Add(-10 * time.Second) // decayed to ~1 token
	a.table.mu.Unlock()

	a.Saturation(context.Background(), dlEndpoints("pod1"))
	assert.True(t, isPaused(a, "stale"), "1200 undecayed > 900 ceiling pauses regardless of decay")
	assert.InDelta(t, 900.0, snapshotOf(a, "default/pod1").room, 1e-6, "a paused program leaves both views")
}

// The admission room is the decayed view: a new program can be admitted into
// room that the undecayed view says is occupied by an idle program.
func TestAdmissionUsesDecayedView(t *testing.T) {
	cfg := testConfig()
	cfg.ActingHalfLifeSeconds = 1
	a := newTestAgent(cfg)
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "old-idle", sched[0], 500)
	seedProgram(t, a, "fresh-idle", sched[0], 350) // 850 undecayed: under the ceiling, no pause
	a.table.mu.Lock()
	a.table.programs["old-idle"].lastResponseAt = time.Now().Add(-10 * time.Second) // ~0.5 tokens
	a.table.mu.Unlock()
	a.Saturation(context.Background(), dlEndpoints("pod1"))

	snap := snapshotOf(a, "default/pod1")
	assert.InDelta(t, 850.0, snap.tokens, 1e-6, "pause view counts the idle program in full")
	assert.InDelta(t, 549.5, snap.room, 1.0, "admission view has decayed it away")

	q := makeQueueWithBytes("newbie", 1, time.Now(), 500*4) // 500 tokens: fits 549, not 50
	picked, err := a.Pick(context.Background(), bandOf(q))
	require.NoError(t, err)
	assert.Same(t, q, picked, "admission is decided on the decayed view")
}

func TestSweepIsPerPod(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1", "pod2")

	seedProgram(t, a, "over", sched[0], 1200)
	seedProgram(t, a, "fine", sched[1], 500)
	a.Saturation(context.Background(), dlEndpoints("pod1", "pod2"))

	assert.True(t, isPaused(a, "over"))
	assert.False(t, isPaused(a, "fine"), "a pod under its ceiling is never swept")
	assert.Equal(t, 500.0, snapshotOf(a, "default/pod2").tokens)
}

// Upstream's capacity check adds BUFFER_PER_PROGRAM per active program, so
// buffers alone can push a pod over and trigger pauses.
func TestSweepCountsPerProgramBuffers(t *testing.T) {
	cfg := testConfig()
	cfg.BufferTokensPerProgram = 100
	a := newTestAgent(cfg)
	sched := newTestEndpoints("pod1")

	// 3 x 250 = 750 tokens plus 3 x 100 buffers = 1050 > 900. Pausing the
	// smallest (250 + its buffer) leaves 700.
	for _, id := range []string{"a", "b", "c"} {
		seedProgram(t, a, id, sched[0], 250)
	}
	a.Saturation(context.Background(), dlEndpoints("pod1"))

	assert.Equal(t, 1.0, testutil.ToFloat64(a.metrics.pauses))
	assert.Equal(t, 700.0, snapshotOf(a, "default/pod1").tokens)
}

// A pause mark survives an overlapping turn: the program pauses once all its
// turns drain (upstream pauses on the first completed response; this port
// waits for the last).
func TestMarkPersistsAcrossOverlappingTurns(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "a", sched[0], 600)
	seedProgram(t, a, "b", sched[0], 500)
	first := inflightRequest(t, a, "a", sched[0], 400)
	inflightRequest(t, a, "b", sched[0], 400) // 1100 > 900, nothing idle: both marked
	a.Saturation(context.Background(), dlEndpoints("pod1"))
	require.True(t, isMarked(a, "a"))

	second := inflightRequest(t, a, "a", sched[0], 400) // overlapping turn dispatches as REASONING
	assert.True(t, isMarked(a, "a"), "an overlapping turn does not clear the mark")

	a.ResponseBody(context.Background(), first, endOfStream(650, 600), nil)
	assert.False(t, isPaused(a, "a"), "still one turn in flight")
	assert.True(t, isMarked(a, "a"))

	a.ResponseBody(context.Background(), second, endOfStream(700, 650), nil)
	assert.True(t, isPaused(a, "a"), "the mark matures when the last turn drains")
}

// A marked program whose final turn completes is released, not paused: its
// capacity is freed for good rather than parked.
func TestMarkedProgramFinalTurnReleases(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "a", sched[0], 600)
	seedProgram(t, a, "b", sched[0], 500)
	inflightRequest(t, a, "b", sched[0], 400)
	final := newRequest("a", 400)
	final.Headers = map[string]string{"x-session-final": "true"}
	require.NoError(t, a.PreRequest(context.Background(), final, schedulingResultFor(sched[0])))
	a.Saturation(context.Background(), dlEndpoints("pod1"))
	require.True(t, isMarked(a, "a"))

	a.ResponseBody(context.Background(), final, endOfStream(650, 600), nil)

	a.table.mu.Lock()
	_, tracked := a.table.programs["a"]
	a.table.mu.Unlock()
	assert.False(t, tracked, "released")
	assert.Equal(t, 0.0, testutil.ToFloat64(a.metrics.pauses), "a release is not a pause")
	assert.Equal(t, 1.0, testutil.ToFloat64(a.metrics.sessionFinalReleases))
}

func TestPausedFailsOpenWithoutFitView(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "veteran", sched[0], 5000)
	forcePause(a, "veteran")

	q := makeQueueWithBytes("veteran", 1, time.Now(), 5000*4)
	picked, err := a.Pick(context.Background(), bandOf(q))
	require.NoError(t, err)
	assert.Same(t, q, picked, "with no fit view (plugin not the detector) only class order applies")
}

// The forced-admission backstop applies to paused programs too (upstream
// _wait_for_resume force-resumes to the origin backend): no reservation is
// made and the sticky binding places it.
func TestStarvingPausedProgramForceAdmittedToOrigin(t *testing.T) {
	cfg := testConfig()
	cfg.HeadWaitStarvationMs = 100
	a := newTestAgent(cfg)
	sched := newTestEndpoints("pod1", "pod2")

	seedProgram(t, a, "veteran", sched[0], 400)
	seedProgram(t, a, "runner", sched[0], 900) // pod1 full once veteran is paused
	seedProgram(t, a, "other", sched[1], 900)  // pod2 full too
	forcePause(a, "veteran")
	primeFitView(a, "pod1", "pod2")

	q := makeQueueWithBytes("veteran", 1, time.Now().Add(-time.Second), 400*4)
	picked, err := a.Pick(context.Background(), bandOf(q))
	require.NoError(t, err)
	require.Same(t, q, picked, "force-admitted past the bound although nothing fits")
	_, reserved := reservationOf(a, "veteran")
	assert.False(t, reserved, "a forced admission fit no pod, so nothing is reserved")
	assert.Equal(t, 1.0, testutil.ToFloat64(a.metrics.starvationPromotions))

	scores := a.Score(context.Background(), newRequest("veteran", 400*4), sched)
	assert.Equal(t, 1.0, scores[sched[0]], "the sticky origin places a force-admitted paused program")
}

func TestExpiredReservationFreesRoom(t *testing.T) {
	a := newTestAgent(testConfig())
	primeFitView(a, "pod1")

	a.table.mu.Lock()
	a.table.pending["ghost"] = pendingAdmission{tokens: 900, pod: "default/pod1", at: time.Now().Add(-2 * pendingAdmissionTTL)}
	a.table.mu.Unlock()

	q := makeQueueWithBytes("newbie", 1, time.Now(), 400)
	picked, err := a.Pick(context.Background(), bandOf(q))
	require.NoError(t, err)
	assert.Same(t, q, picked, "a reservation whose program never bound expires and stops blocking room")
	_, ghost := reservationOf(a, "ghost")
	assert.False(t, ghost, "expired reservations are dropped")
}

func TestPausedReservationClearedOnBind(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "veteran", sched[0], 300)
	forcePause(a, "veteran")
	primeFitView(a, "pod1")

	q := makeQueueWithBytes("veteran", 1, time.Now(), 300*4)
	picked, err := a.Pick(context.Background(), bandOf(q))
	require.NoError(t, err)
	require.Same(t, q, picked)
	_, reserved := reservationOf(a, "veteran")
	require.True(t, reserved)

	require.NoError(t, a.PreRequest(context.Background(), newRequest("veteran", 300*4), schedulingResultFor(sched[0])))
	_, reserved = reservationOf(a, "veteran")
	assert.False(t, reserved, "binding consumes the reservation; the footprint now counts directly")
	assert.False(t, isPaused(a, "veteran"))
}

// A paused program whose origin pod left the pool is placed by room and
// rebound; the dead binding must not pin it or crash the fit check.
func TestPausedProgramWithVanishedOriginIsPlacedByRoom(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1", "pod2")

	seedProgram(t, a, "veteran", sched[0], 300)
	forcePause(a, "veteran")
	primeFitView(a, "pod2") // pod1 is gone from the pool

	q := makeQueueWithBytes("veteran", 1, time.Now(), 300*4)
	picked, err := a.Pick(context.Background(), bandOf(q))
	require.NoError(t, err)
	require.Same(t, q, picked)
	res, ok := reservationOf(a, "veteran")
	require.True(t, ok)
	assert.Equal(t, "default/pod2", res.pod)

	scores := a.Score(context.Background(), newRequest("veteran", 300*4), sched[1:])
	assert.Equal(t, 1.0, scores[sched[1]])
	require.NoError(t, a.PreRequest(context.Background(), newRequest("veteran", 300*4), schedulingResultFor(sched[1])))
	assert.Equal(t, 1.0, testutil.ToFloat64(a.metrics.rebinds))
}

// The estimate raised while streaming is removed in full on abort, and the
// committed footprint falls back to the prompt estimate, not the streamed
// total.
func TestStreamingAbortReleasesAppliedAmount(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	req := inflightRequest(t, a, "a", sched[0], 2000) // 500 estimate
	a.ResponseBody(context.Background(), req, &fwkrc.Response{StreamedEvents: 40}, nil)
	a.table.mu.Lock()
	inflight := a.table.programs["a"].inflightTokens
	a.table.mu.Unlock()
	require.Equal(t, int64(540), inflight)

	a.ResponseBody(context.Background(), req, &fwkrc.Response{EndOfStream: true}, nil) // abort, no usage
	a.table.mu.Lock()
	st := a.table.programs["a"]
	a.table.mu.Unlock()
	assert.Equal(t, int64(0), st.inflightTokens)
	assert.Equal(t, int64(500), st.committedTokens)
}

// Upstream keeps unreleased programs forever; here the TTL sweep drops a
// paused program too, and it re-enters as new.
func TestPausedProgramIsEvictedByTTL(t *testing.T) {
	cfg := testConfig()
	cfg.EvictionTTLSeconds = 1
	a := newTestAgent(cfg)
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "gone", sched[0], 300)
	forcePause(a, "gone")
	a.evictIdle(time.Now().Add(2 * time.Second))

	a.table.mu.Lock()
	class, tokens := a.table.classAndTokens("gone")
	a.table.mu.Unlock()
	assert.Equal(t, classNew, class)
	assert.Equal(t, 0.0, tokens)
	assert.Equal(t, 1.0, testutil.ToFloat64(a.metrics.ttlEvictions))
}

func TestProgramStateGaugesAndDumpAfterSweep(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "idle-small", sched[0], 200)
	seedProgram(t, a, "idle-big", sched[0], 400)
	inflightRequest(t, a, "runner", sched[0], 400*4) // 400 in flight; 1000 > 900 pauses idle-small
	a.Saturation(context.Background(), dlEndpoints("pod1"))

	assert.Equal(t, 1.0, programsGauge(a, "running"))
	assert.Equal(t, 1.0, programsGauge(a, "idle"))
	assert.Equal(t, 1.0, programsGauge(a, "paused"))
	assert.Equal(t, 0.0, programsGauge(a, "marked"))

	raw, err := a.DumpState()
	require.NoError(t, err)
	var state dumpState
	require.NoError(t, json.Unmarshal(raw, &state))
	assert.Equal(t, 3, state.TotalPrograms)
	assert.Equal(t, 1, state.PausedPrograms)
	assert.Equal(t, int64(1), state.PausesTotal)
	assert.Equal(t, 800.0, state.Pods["default/pod1"].Tokens, "the paused program is out of the pod aggregate")
	assert.Equal(t, 2, state.Pods["default/pod1"].Programs)
}

// shared_tokens applies to both views and only with real scraped capacity.
func TestKVUsageCorrectionAffectsRoomAndNeedsRealCapacity(t *testing.T) {
	cfg := testConfig()
	cfg.KVUsageCorrection = true
	a := newTestAgent(cfg)
	sched := newTestEndpoints("pod1", "pod2")

	inflightRequest(t, a, "runner", sched[0], 20000) // 5000 tokens on an 8000-token pod at 50% usage
	inflightRequest(t, a, "other", sched[1], 20000)  // 5000 tokens on a fallback-capacity pod at 50% usage
	fallback := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: sched[1].GetMetadata().ID},
		&fwkdl.Metrics{KVCacheUsagePercent: 0.5, UpdateTime: time.Now()})
	a.Saturation(context.Background(), []fwkdl.Endpoint{endpointWithUsage("pod1", 16, 500, 0.5, true), fallback})

	real := snapshotOf(a, "default/pod1")
	assert.Equal(t, 4000.0, real.tokens)
	assert.InDelta(t, 7200.0-4000.0, real.room, 1e-6, "the correction also widens the admission room")

	fb := snapshotOf(a, "default/pod2")
	assert.Equal(t, 5000.0, fb.tokens, "no correction without real capacity: the usage percent has no trustworthy base")
}

func TestConfigStarvationGuardDisabledIsValid(t *testing.T) {
	_, err := Factory("test", json.NewDecoder(bytes.NewBufferString(`{"headWaitStarvationMs": 0}`)), nil)
	assert.NoError(t, err, "0 disables the guard and must not trip the eviction-TTL check")
}

func TestNewProgramPlacedOnPodWithMostRoom(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1", "pod2")

	seedProgram(t, a, "a", sched[0], 500) // pod1 room 400
	seedProgram(t, a, "b", sched[1], 200) // pod2 room 700
	primeFitView(a, "pod1", "pod2")

	q := makeQueueWithBytes("newbie", 1, time.Now(), 300*4)
	picked, err := a.Pick(context.Background(), bandOf(q))
	require.NoError(t, err)
	require.Same(t, q, picked)
	res, ok := reservationOf(a, "newbie")
	require.True(t, ok)
	assert.Equal(t, "default/pod2", res.pod)

	scores := a.Score(context.Background(), newRequest("newbie", 300*4), sched)
	assert.Equal(t, 1.0, scores[sched[1]], "the scorer routes to the reserved pod")
	assert.Equal(t, 0.0, scores[sched[0]])
}

// The production adapter attaches the scheduling request to the queue item;
// its body size, not the flow-control byte size, is what the estimate uses.
func TestHeadSizePrefersSchedulingRequest(t *testing.T) {
	a := newTestAgent(testConfig())
	primeFitView(a, "pod1") // room 900

	big := makeQueueWithRequest("newbie", time.Now(), newRequest("newbie", 4000), 400) // 1000 tokens by body, 100 by byte size
	picked, err := a.Pick(context.Background(), bandOf(big))
	require.NoError(t, err)
	assert.Nil(t, picked, "the 1000-token body does not fit; the 400-byte flow size is not used")

	small := makeQueueWithRequest("newbie", time.Now(), newRequest("newbie", 400), 4000)
	picked, err = a.Pick(context.Background(), bandOf(small))
	require.NoError(t, err)
	assert.Same(t, small, picked)
}

func TestPick_PausedOrderSmallestThenOldest(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")
	now := time.Now()

	for id, tokens := range map[string]int{"p200": 200, "p300": 300, "p200-old": 200} {
		seedProgram(t, a, id, sched[0], tokens)
		forcePause(a, id)
	}
	primeFitView(a, "pod1")

	picked, err := a.Pick(context.Background(), bandOf(
		makeQueueWithBytes("p300", 1, now.Add(-time.Minute), 300*4),
		makeQueueWithBytes("p200", 1, now, 200*4),
		makeQueueWithBytes("p200-old", 1, now.Add(-time.Second), 200*4),
	))
	require.NoError(t, err)
	assert.Equal(t, "p200-old", picked.FlowKey().ID, "smallest footprint first, oldest head among equals")
}

// Accounting invariants under a random workload of overlapping turns,
// streaming, sweeps, final turns and evictions. These are the properties the
// admission math depends on; a leak in any of them silently corrupts every
// later decision.
func TestAccountingInvariantsUnderRandomWorkload(t *testing.T) {
	const (
		pods     = 3
		programs = 12
		steps    = 3000
	)
	cfg := testConfig()
	cfg.ActingHalfLifeSeconds = 0.05
	cfg.BufferTokensPerProgram = 20
	cfg.UtilThreshold = 0.8
	cfg.EvictionTTLSeconds = 0.2
	a := newTestAgent(cfg)
	rng := rand.New(rand.NewSource(20260917))

	podNames := make([]string, pods)
	for i := range podNames {
		podNames[i] = fmt.Sprintf("pod%d", i)
	}
	sched := newTestEndpoints(podNames...)
	dl := dlEndpoints(podNames...)

	type open struct {
		req     *fwksched.InferenceRequest
		applied int64
	}
	inflightByProgram := make(map[string][]*open)

	check := func(step int) {
		t.Helper()
		a.table.mu.Lock()
		defer a.table.mu.Unlock()
		now := time.Now()
		expected := make(map[string]podLoad)
		for id, st := range a.table.programs {
			require.GreaterOrEqual(t, st.inflightTokens, int64(0), "step %d: %s negative inflight", step, id)
			var sum int64
			for _, o := range inflightByProgram[id] {
				sum += o.applied
			}
			require.Equal(t, sum, st.inflightTokens, "step %d: %s inflight != applied amounts of its open turns", step, id)
			require.False(t, st.paused && st.markedForPause, "step %d: %s both paused and marked", step, id)
			if st.paused {
				require.Equal(t, int64(0), st.inflightTokens, "step %d: %s paused with a turn in flight", step, id)
			}
			if st.markedForPause {
				require.Greater(t, st.inflightTokens, int64(0), "step %d: %s marked with nothing in flight", step, id)
			}
			if st.paused {
				continue
			}
			l := expected[st.podName]
			l.undecayed += footprint(st)
			l.decayed += a.table.decayedFootprint(st, now)
			l.programs++
			expected[st.podName] = l
		}
		got := a.table.podLoads(now)
		require.Equal(t, len(expected), len(got), "step %d: pod set", step)
		for pod, e := range expected {
			require.InDelta(t, e.undecayed, got[pod].undecayed, 1e-6, "step %d: %s undecayed", step, pod)
			require.Equal(t, e.programs, got[pod].programs, "step %d: %s program count", step, pod)
			require.LessOrEqual(t, got[pod].decayed, got[pod].undecayed+1e-6, "step %d: %s decayed above undecayed", step, pod)
		}
		for pod, snap := range a.table.snapshot {
			require.LessOrEqual(t, snap.room, snap.capacity*a.utilThreshold+1e-6, "step %d: %s room above ceiling", step, pod)
		}
		require.Equal(t, float64(a.table.pausesTotal), testutil.ToFloat64(a.metrics.pauses), "step %d: pause counter drift", step)
	}

	for step := 0; step < steps; step++ {
		id := fmt.Sprintf("prog%d", rng.Intn(programs))
		switch rng.Intn(10) {
		case 0, 1, 2: // start a turn on a random pod (overlapping turns allowed, at most 2)
			if len(inflightByProgram[id]) >= 2 {
				continue
			}
			req := newRequest(id, 100+rng.Intn(4000))
			if rng.Intn(25) == 0 {
				req.Headers = map[string]string{"x-session-final": "true"}
			}
			require.NoError(t, a.PreRequest(context.Background(), req, schedulingResultFor(sched[rng.Intn(pods)])))
			st, _ := fwksched.ReadRequestAttribute[inflightState](req, inflightStateKey)
			inflightByProgram[id] = append(inflightByProgram[id], &open{req: req, applied: st.applied})
		case 3: // stream a chunk on an open turn
			opens := inflightByProgram[id]
			if len(opens) == 0 {
				continue
			}
			o := opens[rng.Intn(len(opens))]
			events := rng.Intn(80)
			a.ResponseBody(context.Background(), o.req, &fwkrc.Response{StreamedEvents: events}, nil)
			st, _ := fwksched.ReadRequestAttribute[inflightState](o.req, inflightStateKey)
			o.applied = st.applied
		case 4, 5, 6: // end an open turn, with or without usage
			opens := inflightByProgram[id]
			if len(opens) == 0 {
				continue
			}
			i := rng.Intn(len(opens))
			o := opens[i]
			resp := &fwkrc.Response{EndOfStream: true}
			if rng.Intn(4) != 0 {
				total := 50 + rng.Intn(1200)
				resp = endOfStream(total, total-10)
			}
			a.ResponseBody(context.Background(), o.req, resp, nil)
			inflightByProgram[id] = append(opens[:i], opens[i+1:]...)
			if len(inflightByProgram[id]) == 0 {
				delete(inflightByProgram, id)
			}
		case 7, 8: // the detector cycle
			a.Saturation(context.Background(), dl)
		case 9: // the eviction sweep, occasionally in the future
			a.evictIdle(time.Now().Add(time.Duration(rng.Intn(400)) * time.Millisecond))
			// Evicted programs have no open turns by construction; drop any
			// bookkeeping for programs the table no longer knows.
			a.table.mu.Lock()
			for pid := range inflightByProgram {
				if _, ok := a.table.programs[pid]; !ok {
					delete(inflightByProgram, pid)
				}
			}
			a.table.mu.Unlock()
		}
		check(step)
	}
	assert.Positive(t, testutil.ToFloat64(a.metrics.pauses), "the workload should have exercised the sweep")
}
