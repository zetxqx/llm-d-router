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

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkfc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwkfcmocks "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol/mocks"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// testCapacity is every test pod's KV capacity in tokens: round numbers so
// tests can assert exact values, with byte estimates at the initial 4 bytes
// per token. Tests run the default config: 30s idle lease, full-capacity
// ceiling.
const testCapacity = 1000

func newTestAgent(cfg Config) *ThunderAgent {
	return newThunderAgent("test", cfg)
}

// schedEndpoint builds the scheduling endpoint a request is picked onto.
func schedEndpoint(name string) fwksched.Endpoint {
	nn := types.NamespacedName{Namespace: "default", Name: name}
	return fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: nn}, &fwkdl.Metrics{}, nil)
}

func schedulingResultFor(endpoint fwksched.Endpoint) *fwksched.SchedulingResult {
	return &fwksched.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults: map[string]*fwksched.ProfileRunResult{
			"default": {TargetEndpoints: []fwksched.Endpoint{endpoint}},
		},
	}
}

func newRequest(sessionID string, sizeBytes int) *fwksched.InferenceRequest {
	return &fwksched.InferenceRequest{
		RequestID:        "req-" + sessionID,
		FairnessID:       sessionID,
		RequestSizeBytes: sizeBytes,
	}
}

func endOfStream(totalTokens, promptTokens int) *fwkrc.Response {
	return &fwkrc.Response{
		EndOfStream: true,
		Usage:       requesthandling.Usage{TotalTokens: totalTokens, PromptTokens: promptTokens},
	}
}

// startTurn dispatches one turn without completing it, first reporting the
// endpoint's pod so the ledger knows it.
func startTurn(t *testing.T, a *ThunderAgent, id string, endpoint fwksched.Endpoint, sizeBytes int) *fwksched.InferenceRequest {
	t.Helper()
	primeFitView(a, dlEndpoint(endpoint.GetMetadata().ID.Name))
	req := newRequest(id, sizeBytes)
	require.NoError(t, a.PreRequest(context.Background(), req, schedulingResultFor(endpoint)))
	return req
}

// runTurn dispatches and completes one turn reporting totalTokens.
func runTurn(t *testing.T, a *ThunderAgent, id string, endpoint fwksched.Endpoint, sizeBytes, totalTokens int) {
	t.Helper()
	req := startTurn(t, a, id, endpoint, sizeBytes)
	a.ResponseBody(context.Background(), req, endOfStream(totalTokens, totalTokens), nil)
}

func sessionOf(a *ThunderAgent, id string) (session, bool) {
	a.mgr.mu.Lock()
	defer a.mgr.mu.Unlock()
	s, ok := a.mgr.sessions[id]
	if !ok {
		return session{}, false
	}
	return *s, true
}

func podTokens(a *ThunderAgent, pod string) float64 {
	a.mgr.mu.Lock()
	defer a.mgr.mu.Unlock()
	p, ok := a.mgr.endpoints[pod]
	if !ok {
		return -1
	}
	return p.occupancy(time.Now())
}

// idleFor backdates a session's last response, so it looks idle for d.
func idleFor(a *ThunderAgent, id string, d time.Duration) {
	a.mgr.mu.Lock()
	a.mgr.sessions[id].lastResponseAt = time.Now().Add(-d)
	a.mgr.mu.Unlock()
}

// pause marks a session paused, as a reclaim would.
func pause(a *ThunderAgent, id string) {
	a.mgr.mu.Lock()
	a.mgr.sessions[id].paused = true
	a.mgr.mu.Unlock()
}

// forceMaintenance backdates the rate limiter so the next hook call runs the
// full-table maintenance.
func forceMaintenance(a *ThunderAgent) {
	a.mgr.mu.Lock()
	a.mgr.lastMaintenance = time.Time{}
	a.mgr.mu.Unlock()
}

// dlEndpoint builds a pool endpoint reporting testCapacity tokens.
func dlEndpoint(name string) fwkdl.Endpoint {
	return dlEndpointWithMetrics(name, &fwkdl.Metrics{CacheBlockSize: 1, CacheNumBlocks: testCapacity})
}

func dlEndpointWithMetrics(name string, metrics *fwkdl.Metrics) fwkdl.Endpoint {
	nn := types.NamespacedName{Namespace: "default", Name: name}
	return fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: nn}, metrics)
}

// primeFitView refreshes the pod ledger the way the flow controller does
// each dispatch cycle.
func primeFitView(a *ThunderAgent, endpoints ...fwkdl.Endpoint) {
	a.Saturation(context.Background(), endpoints)
}

func isPaused(a *ThunderAgent, id string) bool {
	a.mgr.mu.Lock()
	defer a.mgr.mu.Unlock()
	s, ok := a.mgr.sessions[id]
	return ok && s.paused
}

func makeQueue(id string, headEnqueue time.Time, headBytes uint64) *fwkfcmocks.MockFlowQueueAccessor {
	return &fwkfcmocks.MockFlowQueueAccessor{
		LenV:     1,
		FlowKeyV: fwkfc.FlowKey{ID: id},
		PeekV: &fwkfcmocks.MockQueueItemAccessor{
			EnqueueTimeV:     headEnqueue,
			OriginalRequestV: fwkfcmocks.NewMockFlowControlRequest(headBytes, "req-"+id, fwkfc.FlowKey{ID: id}),
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
