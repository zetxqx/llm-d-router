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

package flowcontrol_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	contractmocks "github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/contracts/mocks"
	fcTypes "github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/types"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/thunderagent"
)

// These tests wire one ThunderAgent instance into the real FlowController as
// both the saturation detector and the fairness policy, then drive program
// state through the plugin's public PreRequest/ResponseBody hooks. They cover
// the closed loop the plugin's unit tests cannot: a request held by
// EnqueueAndWait while the program working set is over threshold, and its
// release when capacity frees through a turn completing, through acting
// decay, and in REASONING-before-NEW order.

const thunderTestPod = "default/pod-1"

func newThunderForIntegration(t *testing.T, params string) *thunderagent.ThunderAgent {
	t.Helper()
	// A nil handle skips metrics registration and the eviction sweep, neither
	// of which these tests need.
	plugin, err := thunderagent.Factory(t.Name(), fwkplugin.StrictDecoder([]byte(params)), nil)
	require.NoError(t, err)
	return plugin.(*thunderagent.ThunderAgent)
}

func thunderCandidates() *contractmocks.MockEndpointCandidates {
	meta := &datalayer.EndpointMetadata{ID: types.NamespacedName{Namespace: "default", Name: "pod-1"}}
	return &contractmocks.MockEndpointCandidates{
		Candidates: []datalayer.Endpoint{datalayer.NewEndpoint(meta, datalayer.NewMetrics())},
	}
}

// runTurn drives one full turn for a program: PreRequest binds it to the test
// pod and ResponseBody commits totalTokens. The program is left idle with
// dispatchCount > 0, so it classifies as REASONING with a committed footprint.
func runTurn(t *testing.T, ta *thunderagent.ThunderAgent, programID string, totalTokens int) {
	t.Helper()
	endpoint := fwksched.NewEndpoint(
		&datalayer.EndpointMetadata{ID: types.NamespacedName{Namespace: "default", Name: "pod-1"}},
		&datalayer.Metrics{}, nil)
	req := &fwksched.InferenceRequest{RequestID: "seed-" + programID, FairnessID: programID}
	result := &fwksched.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults: map[string]*fwksched.ProfileRunResult{
			"default": {TargetEndpoints: []fwksched.Endpoint{endpoint}},
		},
	}
	require.NoError(t, ta.PreRequest(context.Background(), req, result))
	ta.ResponseBody(context.Background(), req,
		&fwkrc.Response{EndOfStream: true, Usage: requesthandling.Usage{TotalTokens: totalTokens, PromptTokens: totalTokens}}, nil)
}

// startTurn drives PreRequest for a program on the test pod without completing
// the turn, leaving the program with a request in flight (upstream REASONING
// on GPU). endTurn completes it with the given usage.
func startTurn(t *testing.T, ta *thunderagent.ThunderAgent, programID string, sizeBytes int) *fwksched.InferenceRequest {
	t.Helper()
	endpoint := fwksched.NewEndpoint(
		&datalayer.EndpointMetadata{ID: types.NamespacedName{Namespace: "default", Name: "pod-1"}},
		&datalayer.Metrics{}, nil)
	req := &fwksched.InferenceRequest{RequestID: "turn-" + programID, FairnessID: programID, RequestSizeBytes: sizeBytes}
	result := &fwksched.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults: map[string]*fwksched.ProfileRunResult{
			"default": {TargetEndpoints: []fwksched.Endpoint{endpoint}},
		},
	}
	require.NoError(t, ta.PreRequest(context.Background(), req, result))
	return req
}

func endTurn(ta *thunderagent.ThunderAgent, req *fwksched.InferenceRequest, totalTokens int) {
	ta.ResponseBody(context.Background(), req,
		&fwkrc.Response{EndOfStream: true, Usage: requesthandling.Usage{TotalTokens: totalTokens, PromptTokens: totalTokens}}, nil)
}

// enqueueTurn submits one queued turn for a program through the real
// controller and reports its outcome on the returned channel.
func enqueueTurn(h *integrationHarness, programID string) <-chan dispatchResult {
	done := make(chan dispatchResult, 1)
	go func() {
		req := &testRequest{
			id:       "turn-" + programID,
			key:      flowcontrol.FlowKey{ID: programID, Priority: 0},
			byteSize: 100,
			ttl:      5 * time.Minute,
		}
		outcome, err := h.fc.EnqueueAndWait(h.ctx, req)
		done <- dispatchResult{id: programID, outcome: outcome, err: err}
	}()
	return done
}

func requireHeld(t *testing.T, done <-chan dispatchResult, d time.Duration) {
	t.Helper()
	select {
	case r := <-done:
		t.Fatalf("request for %s should be held while the pool is saturated, got outcome %v (err %v)", r.id, r.outcome, r.err)
	case <-time.After(d):
	}
}

func requireDispatched(t *testing.T, done <-chan dispatchResult, d time.Duration) dispatchResult {
	t.Helper()
	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Equal(t, fcTypes.QueueOutcomeDispatched, r.outcome)
		return r
	case <-time.After(d):
		t.Fatal("held request was not dispatched after capacity freed")
		return dispatchResult{}
	}
}

// A pod at 1000-token capacity with utilThreshold 0.9 admits new programs
// while the working set plus their estimate stays within 900 tokens. The
// pause sweep is parked (3600 s) so these tests exercise the admission view
// alone; the pause path has its own test below.
const thunderParams = `{
	"capacityTokens": 1000,
	"utilThreshold": 0.9,
	"actingHalfLifeSeconds": %g,
	"bufferTokensPerProgram": 0,
	"headWaitStarvationMs": %g,
	"evictionTtlSeconds": 3600,
	"evictionSweepSeconds": 300,
	"sessionFinalHeader": "x-session-final",
	"pauseSweepSeconds": 3600
}`

func TestThunderAgentHoldsUntilTurnCompletes(t *testing.T) {
	t.Parallel()
	ta := newThunderForIntegration(t, fmt.Sprintf(thunderParams, 0.0, 30000.0))
	h := newHarness(t, harnessOpts{
		detector:           ta,
		fairness:           ta,
		endpointCandidates: thunderCandidates(),
	})

	// An admitted program fills the pod's admission room. Saturation never
	// gates (it returns 0.0 and only refreshes the fit view); the hold below
	// comes from the fairness policy's fit check.
	runTurn(t, ta, "saturator", 900)
	require.Equal(t, 0.0, ta.Saturation(h.ctx, thunderCandidates().Candidates))

	// The next program's first turn is held at admission.
	done := enqueueTurn(h, "program-b")
	requireHeld(t, done, 300*time.Millisecond)

	// The saturator's next turn completes small: the working set drops below
	// the release point and the held turn dispatches.
	runTurn(t, ta, "saturator", 100)
	r := requireDispatched(t, done, 5*time.Second)
	assert.Equal(t, "program-b", r.id)
}

func TestThunderAgentActingDecayReleasesHold(t *testing.T) {
	t.Parallel()
	ta := newThunderForIntegration(t, fmt.Sprintf(thunderParams, 0.5, 30000.0))
	h := newHarness(t, harnessOpts{
		detector:           ta,
		fairness:           ta,
		endpointCandidates: thunderCandidates(),
	})

	// The saturator is idle (between turns) at 3x the pod's capacity. With a
	// 0.5s half-life its decayed footprint falls below the 800-token release
	// point after roughly one second.
	runTurn(t, ta, "saturator", 3000)

	done := enqueueTurn(h, "program-b")
	requireHeld(t, done, 300*time.Millisecond)

	// No turn completes; decay alone releases the hold.
	requireDispatched(t, done, 10*time.Second)
}

func TestThunderAgentResumesReasoningBeforeNew(t *testing.T) {
	t.Parallel()
	// The starvation guard is disabled so class order alone decides.
	ta := newThunderForIntegration(t, fmt.Sprintf(thunderParams, 0.0, 0.0))
	h := newHarness(t, harnessOpts{
		detector:           ta,
		fairness:           ta,
		endpointCandidates: thunderCandidates(),
	})

	// A veteran program holds KV from an earlier turn; the saturator fills
	// the rest of the pod.
	runTurn(t, ta, "veteran", 10)
	runTurn(t, ta, "saturator", 890)

	var mu sync.Mutex
	var order []string
	record := func(done <-chan dispatchResult) {
		go func() {
			r := <-done
			mu.Lock()
			order = append(order, r.id)
			mu.Unlock()
		}()
	}

	// The new program enqueues first; FCFS would dispatch it first.
	record(enqueueTurn(h, "program-new"))
	time.Sleep(50 * time.Millisecond)
	record(enqueueTurn(h, "veteran"))
	time.Sleep(300 * time.Millisecond)

	runTurn(t, ta, "saturator", 1)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 2
	}, 5*time.Second, 10*time.Millisecond, "both held turns should dispatch after capacity frees")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"veteran", "program-new"}, order,
		"the in-trajectory program resumes before the new one regardless of enqueue order")
}

// A pod at 1000-token capacity, 900-token admission ceiling, with the pause
// sweep running on every dispatch cycle.
const thunderPauseParams = `{
	"capacityTokens": 1000,
	"utilThreshold": 0.9,
	"actingHalfLifeSeconds": 0,
	"bufferTokensPerProgram": 0,
	"headWaitStarvationMs": 30000,
	"evictionTtlSeconds": 3600,
	"evictionSweepSeconds": 300,
	"sessionFinalHeader": "x-session-final",
	"pauseSweepSeconds": 0
}`

func pausesTotal(t *testing.T, ta *thunderagent.ThunderAgent) int64 {
	t.Helper()
	raw, err := ta.DumpState()
	require.NoError(t, err)
	var state struct {
		PausesTotal int64 `json:"pausesTotal"`
	}
	require.NoError(t, json.Unmarshal(raw, &state))
	return state.PausesTotal
}

// A paused mid-trajectory session and a never-admitted one wait together;
// when capacity frees and both fit, the paused session resumes first even though
// the newcomer is smaller and enqueued earlier.
func TestThunderAgentPausedReturneeResumesBeforeNewcomer(t *testing.T) {
	t.Parallel()
	ta := newThunderForIntegration(t, thunderPauseParams)
	h := newHarness(t, harnessOpts{
		detector:           ta,
		fairness:           ta,
		endpointCandidates: thunderCandidates(),
	})

	// The veteran ran a turn (400 committed); the runner fills the rest.
	// 1300 > 900 puts the pod over the ceiling and the dispatch loop pauses
	// the smaller idle program: the veteran.
	runTurn(t, ta, "veteran", 400)
	runTurn(t, ta, "runner", 900)
	require.Eventually(t, func() bool { return pausesTotal(t, ta) >= 1 }, 5*time.Second, time.Millisecond,
		"the over-ceiling pod should pause its smaller idle program")

	// Both wait: room is 900 - 900 = 0. The newcomer is smaller (est 300 vs
	// the veteran's 400 committed) and enqueues first.
	newcomerDone := enqueueTurn(h, "newcomer")
	time.Sleep(50 * time.Millisecond)
	veteranDone := enqueueTurn(h, "veteran")
	requireHeld(t, newcomerDone, 250*time.Millisecond)

	var mu sync.Mutex
	var order []string
	record := func(done <-chan dispatchResult) {
		go func() {
			r := <-done
			mu.Lock()
			order = append(order, r.id)
			mu.Unlock()
		}()
	}
	record(veteranDone)
	record(newcomerDone)

	// The runner's next turn completes small: room opens for both.
	runTurn(t, ta, "runner", 100)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 2
	}, 5*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"veteran", "newcomer"}, order,
		"the paused mid-trajectory session resumes before the never-admitted one")
}

// The mark path end to end: with every program on the pod running, the
// sweep marks them; a marked program pauses when its turn completes; its next
// turn is then held until the other program finishes and frees room.
func TestThunderAgentMarkedProgramPausesAtEndOfTurnAndIsHeld(t *testing.T) {
	t.Parallel()
	ta := newThunderForIntegration(t, thunderPauseParams)
	h := newHarness(t, harnessOpts{
		detector:           ta,
		fairness:           ta,
		endpointCandidates: thunderCandidates(),
	})

	// The pod crosses the ceiling only while both programs are in flight:
	// the runner (500 committed) starts a turn, then the filler's first turn
	// arrives with a 450-token estimate. 950 > 900 with nothing idle, so the
	// dispatch loop's sweep marks both instead of pausing either. (Seeding
	// the filler with a completed turn first would leave an idle window in
	// which the 1 ms sweep pauses it outright.)
	runTurn(t, ta, "runner", 500)
	runnerReq := startTurn(t, ta, "runner", 400)
	fillerReq := startTurn(t, ta, "filler", 1800)
	time.Sleep(50 * time.Millisecond) // several dispatch cycles
	require.Equal(t, int64(0), pausesTotal(t, ta), "in-flight programs are marked, never paused outright")
	require.Equal(t, 2, dumpField(t, ta, "totalPrograms"))

	// The runner's turn completes: its mark matures into a pause, synchronously.
	endTurn(ta, runnerReq, 500)
	require.Equal(t, int64(1), pausesTotal(t, ta), "a marked program pauses when its turn ends")

	// Its next turn must fit again: 500 into 900 - 450 (filler still running) = 450 does not.
	runnerDone := enqueueTurn(h, "runner")
	requireHeld(t, runnerDone, 300*time.Millisecond)

	// The filler finishes small: room opens and the paused runner resumes.
	endTurn(ta, fillerReq, 100)
	requireDispatched(t, runnerDone, 5*time.Second)
}

// Forced admission of a paused program: nothing frees, and the backstop
// dispatches it once its head has waited headWaitStarvationMs.
func TestThunderAgentForcedAdmissionOfPausedProgram(t *testing.T) {
	t.Parallel()
	ta := newThunderForIntegration(t, thunderPauseParamsStarve)
	h := newHarness(t, harnessOpts{
		detector:           ta,
		fairness:           ta,
		endpointCandidates: thunderCandidates(),
	})

	runTurn(t, ta, "veteran", 400)
	runTurn(t, ta, "runner", 900) // 1300 > 900: the veteran is paused
	require.Eventually(t, func() bool { return pausesTotal(t, ta) >= 1 }, 5*time.Second, time.Millisecond)

	started := time.Now()
	done := enqueueTurn(h, "veteran")
	requireHeld(t, done, 150*time.Millisecond)
	requireDispatched(t, done, 5*time.Second)
	assert.GreaterOrEqual(t, time.Since(started), 300*time.Millisecond,
		"dispatch came from the forced-admission backstop, not from a fit")
}

// Under resumePlacement origin-only the paused program waits for its origin
// pod; on a one-pod pool that is the same hold as most-room, ended here by
// the forced-admission backstop. Proves the strict decoder accepts the knob
// and the origin-only branch runs inside the real dispatch loop.
func TestThunderAgentOriginOnlyForcedAdmissionOfPausedProgram(t *testing.T) {
	t.Parallel()
	ta := newThunderForIntegration(t, thunderPauseParamsOriginOnly)
	h := newHarness(t, harnessOpts{
		detector:           ta,
		fairness:           ta,
		endpointCandidates: thunderCandidates(),
	})

	runTurn(t, ta, "veteran", 400)
	runTurn(t, ta, "runner", 900)
	require.Eventually(t, func() bool { return pausesTotal(t, ta) >= 1 }, 5*time.Second, time.Millisecond)

	started := time.Now()
	done := enqueueTurn(h, "veteran")
	requireHeld(t, done, 150*time.Millisecond)
	requireDispatched(t, done, 5*time.Second)
	assert.GreaterOrEqual(t, time.Since(started), 300*time.Millisecond)
	assert.Equal(t, 0, dumpField(t, ta, "originWaitsTotal"), "no other pod had room, so the hold was not the policy's")
}

// The urgent tier (100 ms here) frees a paused program from origin-only
// placement and reorders it, but never bypasses the fit check: on a one-pod
// pool with no room the hold still ends only at the 300 ms backstop.
func TestThunderAgentUrgentTierDoesNotBypassFit(t *testing.T) {
	t.Parallel()
	ta := newThunderForIntegration(t, thunderPauseParamsUrgent)
	h := newHarness(t, harnessOpts{
		detector:           ta,
		fairness:           ta,
		endpointCandidates: thunderCandidates(),
	})

	runTurn(t, ta, "veteran", 400)
	runTurn(t, ta, "runner", 900)
	require.Eventually(t, func() bool { return pausesTotal(t, ta) >= 1 }, 5*time.Second, time.Millisecond)

	started := time.Now()
	done := enqueueTurn(h, "veteran")
	requireHeld(t, done, 150*time.Millisecond)
	requireDispatched(t, done, 5*time.Second)
	assert.GreaterOrEqual(t, time.Since(started), 300*time.Millisecond)
	assert.Equal(t, 0, dumpField(t, ta, "urgentPromotionsTotal"), "urgent but nowhere to go: no urgent dispatch")
}

// On a two-pod pool the urgent tier ends an origin-only hold as soon as the
// program has waited urgentWaitMs: the veteran, paused on a full pod-1, is
// dispatched to the empty pod-2 between 100 ms and the 300 ms backstop.
func TestThunderAgentUrgentMovesToPodWithRoom(t *testing.T) {
	t.Parallel()
	ta := newThunderForIntegration(t, thunderPauseParamsUrgent)
	h := newHarness(t, harnessOpts{
		detector:           ta,
		fairness:           ta,
		endpointCandidates: thunderCandidatesTwoPods(),
	})

	runTurn(t, ta, "veteran", 400)
	runTurn(t, ta, "runner", 900) // pod-1: 1300 > 900, the veteran is paused
	require.Eventually(t, func() bool { return pausesTotal(t, ta) >= 1 }, 5*time.Second, time.Millisecond)

	started := time.Now()
	done := enqueueTurn(h, "veteran")
	requireHeld(t, done, 50*time.Millisecond)
	requireDispatched(t, done, 5*time.Second)
	elapsed := time.Since(started)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond, "held until the urgent threshold")
	assert.Less(t, elapsed, 300*time.Millisecond, "released by the urgent tier, not the backstop")
	assert.Equal(t, 1, dumpField(t, ta, "urgentPromotionsTotal"))
}

func thunderCandidatesTwoPods() *contractmocks.MockEndpointCandidates {
	var eps []datalayer.Endpoint
	for _, name := range []string{"pod-1", "pod-2"} {
		meta := &datalayer.EndpointMetadata{ID: types.NamespacedName{Namespace: "default", Name: name}}
		eps = append(eps, datalayer.NewEndpoint(meta, datalayer.NewMetrics()))
	}
	return &contractmocks.MockEndpointCandidates{Candidates: eps}
}

const thunderPauseParamsUrgent = `{
	"capacityTokens": 1000,
	"utilThreshold": 0.9,
	"actingHalfLifeSeconds": 0,
	"bufferTokensPerProgram": 0,
	"urgentWaitMs": 100,
	"headWaitStarvationMs": 300,
	"evictionTtlSeconds": 3600,
	"evictionSweepSeconds": 300,
	"sessionFinalHeader": "x-session-final",
	"pauseSweepSeconds": 0,
	"resumePlacement": "origin-only"
}`

const thunderPauseParamsOriginOnly = `{
	"capacityTokens": 1000,
	"utilThreshold": 0.9,
	"actingHalfLifeSeconds": 0,
	"bufferTokensPerProgram": 0,
	"headWaitStarvationMs": 300,
	"evictionTtlSeconds": 3600,
	"evictionSweepSeconds": 300,
	"sessionFinalHeader": "x-session-final",
	"pauseSweepSeconds": 0,
	"resumePlacement": "origin-only"
}`

// Same pod and ceiling as thunderPauseParams, with a 300 ms forced-admission
// backstop.
const thunderPauseParamsStarve = `{
	"capacityTokens": 1000,
	"utilThreshold": 0.9,
	"actingHalfLifeSeconds": 0,
	"bufferTokensPerProgram": 0,
	"headWaitStarvationMs": 300,
	"evictionTtlSeconds": 3600,
	"evictionSweepSeconds": 300,
	"sessionFinalHeader": "x-session-final",
	"pauseSweepSeconds": 0
}`

func dumpField(t *testing.T, ta *thunderagent.ThunderAgent, field string) int {
	t.Helper()
	raw, err := ta.DumpState()
	require.NoError(t, err)
	var state map[string]any
	require.NoError(t, json.Unmarshal(raw, &state))
	v, _ := state[field].(float64)
	return int(v)
}
