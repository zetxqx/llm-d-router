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

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

// A turn charges its byte estimate while in flight and settles to the
// usage-reported total when it completes.
func TestTurnAccounting(t *testing.T) {
	a := newTestAgent(defaultConfig())
	ep := schedEndpoint("pod-a")

	req := startTurn(t, a, "s1", ep, 400) // 400 bytes / 4 bytes-per-token = 100 tokens
	s, ok := sessionOf(a, "s1")
	require.True(t, ok)
	require.Equal(t, int64(100), s.inflightTokens)
	require.Equal(t, float64(100), podTokens(a, "default/pod-a"))

	a.ResponseBody(context.Background(), req, endOfStream(250, 100), nil)
	s, _ = sessionOf(a, "s1")
	require.Equal(t, int64(0), s.inflightTokens)
	require.Equal(t, int64(250), s.committedTokens)
	require.Equal(t, float64(250), podTokens(a, "default/pod-a"))
}

// An aborted turn (no usage reported) returns its in-flight estimate; the
// estimate becomes the committed floor.
func TestAbortedTurnReleasesInflight(t *testing.T) {
	a := newTestAgent(defaultConfig())
	req := startTurn(t, a, "s1", schedEndpoint("pod-a"), 400)
	a.ResponseBody(context.Background(), req, endOfStream(0, 0), nil)

	s, _ := sessionOf(a, "s1")
	require.Equal(t, int64(0), s.inflightTokens)
	require.Equal(t, int64(100), s.committedTokens)
}

// During a later turn the footprint is the max of the committed total and
// the new in-flight estimate, not their sum: the new prefill covers the same
// history.
func TestFootprintIsMaxNotSum(t *testing.T) {
	a := newTestAgent(defaultConfig())
	ep := schedEndpoint("pod-a")

	req := startTurn(t, a, "s1", ep, 400)
	// Complete with prompt tokens matching the initial 4 bytes-per-token
	// ratio, so the estimator stays put and later estimates are exact.
	a.ResponseBody(context.Background(), req, endOfStream(300, 100), nil)

	_ = startTurn(t, a, "s1", ep, 800) // in-flight estimate 200 < committed 300
	require.Equal(t, float64(300), podTokens(a, "default/pod-a"))

	_ = startTurn(t, a, "s1", ep, 2000) // second overlapping turn: in-flight 200+500=700 > 300
	require.Equal(t, float64(700), podTokens(a, "default/pod-a"))
}

// Idle sessions past the TTL are dropped by maintenance.
func TestTTLEviction(t *testing.T) {
	a := newTestAgent(defaultConfig())
	runTurn(t, a, "s1", schedEndpoint("pod-a"), 400, 300)

	idleFor(a, "s1", 2*a.mgr.ttl)
	forceMaintenance(a)
	primeFitView(a, dlEndpoint("pod-a"))
	_, ok := sessionOf(a, "s1")
	require.False(t, ok)
}

// A session with a turn in flight is never TTL-evicted, so the completion
// bookkeeping always finds its state.
func TestTTLKeepsInflightSessions(t *testing.T) {
	a := newTestAgent(defaultConfig())
	_ = startTurn(t, a, "s1", schedEndpoint("pod-a"), 400)

	idleFor(a, "s1", 2*a.mgr.ttl)
	forceMaintenance(a)
	primeFitView(a, dlEndpoint("pod-a"))
	_, ok := sessionOf(a, "s1")
	require.True(t, ok)
}

// The TTL is at least an hour and outlasts twice the starvation deadline, so
// a held session is never dropped while it waits.
func TestSessionTTLCoversStarvation(t *testing.T) {
	require.Equal(t, time.Hour, newTestAgent(defaultConfig()).mgr.ttl)

	cfg := defaultConfig()
	cfg.HeadWaitStarvationMs = float64((2 * time.Hour).Milliseconds())
	require.Equal(t, 4*time.Hour, newTestAgent(cfg).mgr.ttl)
}

// Only pods that report their KV capacity enter the ledger; a request picked
// onto any other pod is not tracked.
func TestPodWithoutCapacityIsNotTracked(t *testing.T) {
	a := newTestAgent(defaultConfig())
	primeFitView(a,
		dlEndpointWithMetrics("pod-a", &fwkdl.Metrics{CacheBlockSize: 16, CacheNumBlocks: 1000}),
		dlEndpointWithMetrics("pod-b", &fwkdl.Metrics{}))
	a.mgr.mu.Lock()
	capacity := a.mgr.endpoints["default/pod-a"].capacity
	_, known := a.mgr.endpoints["default/pod-b"]
	a.mgr.mu.Unlock()
	require.Equal(t, float64(16000), capacity)
	require.False(t, known)

	req := newRequest("s1", 400)
	require.NoError(t, a.PreRequest(context.Background(), req, schedulingResultFor(schedEndpoint("pod-b"))))
	a.ResponseBody(context.Background(), req, endOfStream(300, 100), nil)
	_, ok := sessionOf(a, "s1")
	require.False(t, ok)
}

// The estimator converges toward observed bytes-per-token and stays inside
// its clamps.
func TestEstimatorRefinesAndClamps(t *testing.T) {
	a := newTestAgent(defaultConfig())
	ep := schedEndpoint("pod-a")

	runTurn(t, a, "s1", ep, 1000, 100) // sample: 10 bytes/token
	a.mgr.mu.Lock()
	ratio := a.mgr.bytesPerToken
	a.mgr.mu.Unlock()
	require.InDelta(t, 0.8*4.0+0.2*10.0, ratio, 1e-9)

	for i := 0; i < 100; i++ {
		runTurn(t, a, "s1", ep, 1000000, 1) // absurd sample, must clamp
	}
	a.mgr.mu.Lock()
	ratio = a.mgr.bytesPerToken
	a.mgr.mu.Unlock()
	require.LessOrEqual(t, ratio, maxBytesPerToken)
}

// Anonymous requests (no session identity) are not tracked.
func TestAnonymousIgnored(t *testing.T) {
	a := newTestAgent(defaultConfig())
	anon := newRequest("", 400)
	require.NoError(t, a.PreRequest(context.Background(), anon, schedulingResultFor(schedEndpoint("pod-a"))))
	a.mgr.mu.Lock()
	n := len(a.mgr.sessions)
	a.mgr.mu.Unlock()
	require.Equal(t, 0, n)
}
