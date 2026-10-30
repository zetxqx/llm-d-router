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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkfc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwkfcmocks "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol/mocks"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionstate"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/requestheader/agentidentity"
)

// t0 is a fixed base time so tests do not depend on the wall clock.
var t0 = time.Unix(1_000_000, 0)

// testConfig uses round numbers so tests can assert exact values: 1000-token
// fallback capacity, byte estimates at 4 bytes per token, the default 30 s
// idle lease.
func testConfig() Config {
	cfg := defaultConfig()
	cfg.CapacityTokens = 1000
	return cfg
}

// newTestAgent returns an agent wired to its own session-state registry, both
// reading one fake clock.
func newTestAgent(cfg Config) *ThunderAgent {
	a := newThunderAgent("test", cfg)
	clk := clocktesting.NewFakeClock(t0)
	a.clock = clk
	a.registry.Store(sessionstate.NewSessionStateRegistry(clk))
	return a
}

func registryOf(a *ThunderAgent) *sessionstate.SessionStateRegistry {
	return a.registry.Load()
}

func clockOf(a *ThunderAgent) *clocktesting.FakeClock {
	return a.clock.(*clocktesting.FakeClock)
}

func now(a *ThunderAgent) time.Time {
	return a.clock.Now()
}

// advance moves the shared clock, so every session idles for d.
func advance(a *ThunderAgent, d time.Duration) {
	clockOf(a).Step(d)
}

func podID(pod string) string {
	return "default/" + pod
}

// schedEndpoint builds a scheduling endpoint.
func schedEndpoint(name string) fwksched.Endpoint {
	nn := types.NamespacedName{Namespace: "default", Name: name}
	return fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: nn}, &fwkdl.Metrics{}, nil)
}

// turn is one dispatched turn and the tokens it charged.
type turn struct {
	id     string
	tokens int64
}

// startTurn dispatches one turn without completing it, as the
// session-state-producer's PreRequest does.
func startTurn(a *ThunderAgent, id, pod string, sizeBytes int) turn {
	tokens := max(estimateTokens(sizeBytes), 1)
	registryOf(a).RecordDispatch(id, podID(pod), tokens)
	return turn{id: id, tokens: tokens}
}

// finish completes a turn reporting totalTokens, as the
// session-state-producer's ResponseBody does.
func finish(a *ThunderAgent, tr turn, totalTokens int) {
	registryOf(a).RecordResponse(tr.id, sessionstate.TurnOutcome{
		NaturallyCompleted: true,
		TotalTokens:        int64(totalTokens),
		InFlightTokens:     tr.tokens,
	})
}

// seed runs one 400-byte turn for the session on pod and completes it with
// the given committed total.
func seed(a *ThunderAgent, id, pod string, committed int) {
	finish(a, startTurn(a, id, pod, 400), committed)
}

// primeFitView registers pods with unknown capacity, selecting the fallback,
// as endpoint add events do.
func primeFitView(a *ThunderAgent, pods ...string) {
	for _, pod := range pods {
		registryOf(a).UpsertEndpoint(podID(pod), nil)
	}
}

func stateOf(t *testing.T, a *ThunderAgent, id string) sessionstate.SessionState {
	t.Helper()
	state, ok := registryOf(a).State(id)
	require.True(t, ok, "session %q must be tracked", id)
	return state
}

// endpointTokens returns a pod's working set under the accounting rule, or -1
// if the pod is unknown.
func endpointTokens(a *ThunderAgent, id string) float64 {
	reg := registryOf(a)
	a.mu.Lock()
	defer a.mu.Unlock()
	capacity, ok := a.capacityLocked(reg, id)
	if !ok {
		return -1
	}
	return a.loadLocked(reg, id, capacity, now(a), nil).occupancy
}

// pause marks a session paused on its bound pod, as a reclaim would.
func pause(t *testing.T, a *ThunderAgent, id string) {
	t.Helper()
	state := stateOf(t, a, id)
	a.mu.Lock()
	defer a.mu.Unlock()
	l := a.localLocked(id)
	l.paused, l.pausedEndpoint, l.pausedAtTurns = true, state.BoundEndpoint, state.TurnsTaken
}

func isPaused(a *ThunderAgent, id string) bool {
	state, _ := registryOf(a).State(id)
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.local[id].pausedLive(state)
}

func reservedPod(a *ThunderAgent, id string) string {
	state, _ := registryOf(a).State(id)
	a.mu.Lock()
	defer a.mu.Unlock()
	if l := a.local[id]; l.reservationLive(state, now(a)) {
		return l.reservedEndpoint
	}
	return ""
}

// newRequest builds a request whose session id is published the way the
// agent-identity plugin does; an empty sessionID leaves the request
// anonymous.
func newRequest(sessionID string, sizeBytes int) *fwksched.InferenceRequest {
	req := &fwksched.InferenceRequest{
		RequestID:        "req-" + sessionID,
		RequestSizeBytes: sizeBytes,
	}
	if sessionID != "" {
		req.PutAttribute(agentidentity.AgentIdentityKey, sessionID)
	}
	return req
}

func makeQueue(id string, headEnqueue time.Time, headBytes uint64) *fwkfcmocks.MockFlowQueueAccessor {
	return makeFlowQueue(id, id, headEnqueue, headBytes)
}

// makeFlowQueue builds a one-item queue named flowID whose head request
// carries the given agent-identity session; an empty session leaves it
// anonymous.
func makeFlowQueue(flowID, session string, headEnqueue time.Time, headBytes uint64) *fwkfcmocks.MockFlowQueueAccessor {
	req := fwkfcmocks.NewMockFlowControlRequest(headBytes, "req-"+flowID, fwkfc.FlowKey{ID: flowID})
	req.InferenceRequestV = newRequest(session, int(headBytes))
	return &fwkfcmocks.MockFlowQueueAccessor{
		LenV:     1,
		FlowKeyV: fwkfc.FlowKey{ID: flowID},
		PeekV: &fwkfcmocks.MockQueueItemAccessor{
			EnqueueTimeV:     headEnqueue,
			OriginalRequestV: req,
		},
	}
}

func bandOf(queues ...fwkfc.FlowQueueAccessor) *fwkfcmocks.MockPriorityBandAccessor {
	return &fwkfcmocks.MockPriorityBandAccessor{
		IterateQueuesFunc: func(callback func(flow fwkfc.FlowQueueAccessor) bool) {
			for _, q := range queues {
				if !callback(q) {
					return
				}
			}
		},
	}
}

func pick(t *testing.T, a *ThunderAgent, queues ...fwkfc.FlowQueueAccessor) fwkfc.FlowQueueAccessor {
	t.Helper()
	got, err := a.Pick(context.Background(), bandOf(queues...))
	require.NoError(t, err)
	return got
}
