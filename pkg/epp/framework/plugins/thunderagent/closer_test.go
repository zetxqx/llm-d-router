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
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	fwkplugins "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins"
	"github.com/llm-d/llm-d-router/test/utils"
)

func closeTestConfig() Config {
	cfg := testConfig()
	cfg.CloseSession = true
	return cfg
}

func newCloseTestAgent(t *testing.T, cfg Config) *ThunderAgent {
	t.Helper()
	a := newTestAgent(cfg)
	t.Cleanup(a.closer.close)
	return a
}

type closeServer struct {
	srv      *httptest.Server
	host     string
	port     string
	mu       sync.Mutex
	received []string
	status   int
}

func newCloseServer(t *testing.T) *closeServer {
	t.Helper()
	cs := &closeServer{status: http.StatusOK}
	cs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != closePath || r.Header.Get("Content-Type") != "application/json" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var p closePayload
		if err := json.Unmarshal(raw, &p); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		cs.mu.Lock()
		status := cs.status
		if status >= http.StatusOK && status < http.StatusMultipleChoices {
			cs.received = append(cs.received, p.SessionID)
		}
		cs.mu.Unlock()
		w.WriteHeader(status)
	}))
	u, err := url.Parse(cs.srv.URL)
	require.NoError(t, err)
	cs.host, cs.port, err = net.SplitHostPort(u.Host)
	require.NoError(t, err)
	t.Cleanup(cs.srv.Close)
	return cs
}

func (cs *closeServer) sessions() []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	out := make([]string, len(cs.received))
	copy(out, cs.received)
	return out
}

func (cs *closeServer) schedEndpoint(name string, labels map[string]string) fwksched.Endpoint {
	return fwksched.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:      types.NamespacedName{Namespace: "default", Name: name},
		Address: cs.host,
		Port:    cs.port,
		Labels:  labels,
	}, &fwkdl.Metrics{}, nil)
}

func (cs *closeServer) dlEndpoint(name string, labels map[string]string) fwkdl.Endpoint {
	return fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:      types.NamespacedName{Namespace: "default", Name: name},
		Address: cs.host,
		Port:    cs.port,
		Labels:  labels,
	}, &fwkdl.Metrics{})
}

// PreRequest writes session_id into PayloadMap for ChatCompletions,
// Completions, and Generate on SGLang endpoints, overwriting any client value
// and skipping when session_params is present, on other APIs, on non-SGLang
// endpoints, or when closeSession is disabled.
func TestPreRequestInjectsSessionID(t *testing.T) {
	chatBody := func(extra map[string]any) *fwkrh.InferenceRequestBody {
		m := fwkrh.PayloadMap{"model": "m"}
		for k, v := range extra {
			m[k] = v
		}
		return &fwkrh.InferenceRequestBody{
			ChatCompletions: &fwkrh.ChatCompletionsRequest{},
			Payload:         m,
		}
	}

	sglangUnlabeled := schedEndpoint("pod-a", 0, 0)
	sglangLabeled := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:     types.NamespacedName{Namespace: "default", Name: "pod-a"},
		Labels: map[string]string{fwkplugins.EngineTypeLabelKey: "SGLang"},
	}, &fwkdl.Metrics{}, nil)
	sglangLegacy := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:     types.NamespacedName{Namespace: "default", Name: "pod-a"},
		Labels: map[string]string{legacyEngineTypeLabelKey: "sglang"},
	}, &fwkdl.Metrics{}, nil)
	vllmLabeled := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:     types.NamespacedName{Namespace: "default", Name: "pod-a"},
		Labels: map[string]string{fwkplugins.EngineTypeLabelKey: "vllm"},
	}, &fwkdl.Metrics{}, nil)
	vllmLegacy := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:     types.NamespacedName{Namespace: "default", Name: "pod-a"},
		Labels: map[string]string{legacyEngineTypeLabelKey: "vLLM"},
	}, &fwkdl.Metrics{}, nil)

	t.Run("supported APIs and client session_id overwrite", func(t *testing.T) {
		a := newCloseTestAgent(t, closeTestConfig())
		for name, body := range map[string]*fwkrh.InferenceRequestBody{
			"chat_completions": chatBody(map[string]any{"session_id": "client-supplied"}),
			"completions": {
				Completions: &fwkrh.CompletionsRequest{},
				Payload:     fwkrh.PayloadMap{"model": "m"},
			},
			"generate": {
				Generate: &fwkrh.GenerateRequest{},
				Payload:  fwkrh.PayloadMap{"text": "hi"},
			},
		} {
			req := newRequest("s1", 400)
			req.Body = body
			require.NoError(t, a.PreRequest(context.Background(), req, schedulingResultFor(sglangUnlabeled)), name)
			require.True(t, body.Mutated, name)
			require.Equal(t, "s1", body.Payload.(fwkrh.PayloadMap)["session_id"], name)
		}
	})

	t.Run("explicit and legacy SGLang labels inject", func(t *testing.T) {
		a := newCloseTestAgent(t, closeTestConfig())
		overrideSGLang := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{
			ID: types.NamespacedName{Namespace: "default", Name: "pod-a"},
			Labels: map[string]string{
				fwkplugins.EngineTypeLabelKey: "sglang",
				legacyEngineTypeLabelKey:      "vllm",
			},
		}, &fwkdl.Metrics{}, nil)
		for _, ep := range []fwksched.Endpoint{sglangLabeled, sglangLegacy, overrideSGLang} {
			req := newRequest("s1", 400)
			req.Body = chatBody(nil)
			require.NoError(t, a.PreRequest(context.Background(), req, schedulingResultFor(ep)))
			require.True(t, req.Body.Mutated)
			require.Equal(t, "s1", req.Body.Payload.(fwkrh.PayloadMap)["session_id"])
		}
	})

	t.Run("skipped when session_params present", func(t *testing.T) {
		a := newCloseTestAgent(t, closeTestConfig())
		req := newRequest("s1", 400)
		req.Body = chatBody(map[string]any{"session_params": map[string]any{"id": "custom"}})
		require.NoError(t, a.PreRequest(context.Background(), req, schedulingResultFor(sglangUnlabeled)))
		require.False(t, req.Body.Mutated)
		require.NotContains(t, req.Body.Payload.(fwkrh.PayloadMap), "session_id")
	})

	t.Run("skipped for Responses and vllmhttp generate APIs", func(t *testing.T) {
		a := newCloseTestAgent(t, closeTestConfig())
		for _, body := range []*fwkrh.InferenceRequestBody{
			{
				Responses: &fwkrh.ResponsesRequest{},
				Payload:   fwkrh.PayloadMap{"model": "m"},
			},
			{
				Generate: &fwkrh.GenerateRequest{TokenIDs: []uint32{1, 2}},
				Payload:  fwkrh.PayloadMap{"token_ids": []uint32{1, 2}},
			},
		} {
			req := newRequest("s1", 400)
			req.Body = body
			require.NoError(t, a.PreRequest(context.Background(), req, schedulingResultFor(sglangUnlabeled)))
			require.False(t, req.Body.Mutated)
			require.NotContains(t, req.Body.Payload.(fwkrh.PayloadMap), "session_id")
		}
	})

	t.Run("skipped for vLLM endpoints", func(t *testing.T) {
		a := newCloseTestAgent(t, closeTestConfig())
		overrideVLLM := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{
			ID: types.NamespacedName{Namespace: "default", Name: "pod-a"},
			Labels: map[string]string{
				fwkplugins.EngineTypeLabelKey: "vllm",
				legacyEngineTypeLabelKey:      "sglang",
			},
		}, &fwkdl.Metrics{}, nil)
		for _, ep := range []fwksched.Endpoint{vllmLabeled, vllmLegacy, overrideVLLM} {
			req := newRequest("s1", 400)
			req.Body = chatBody(nil)
			require.NoError(t, a.PreRequest(context.Background(), req, schedulingResultFor(ep)))
			require.False(t, req.Body.Mutated)
			require.NotContains(t, req.Body.Payload.(fwkrh.PayloadMap), "session_id")
		}
	})

	t.Run("skipped when closeSession is false", func(t *testing.T) {
		a := newTestAgent(testConfig())
		req := newRequest("s1", 400)
		req.Body = chatBody(nil)
		require.NoError(t, a.PreRequest(context.Background(), req, schedulingResultFor(sglangUnlabeled)))
		require.False(t, req.Body.Mutated)
		require.NotContains(t, req.Body.Payload.(fwkrh.PayloadMap), "session_id")
	})
}

// Pausing an idle session in Pick sends POST /close_session to its pod;
// resuming the paused session does not send another close.
func TestCloseSessionOnPauseAndNotOnResume(t *testing.T) {
	srv := newCloseServer(t)
	a := newCloseTestAgent(t, closeTestConfig())
	ep := srv.schedEndpoint("pod-a", nil)
	dlEp := srv.dlEndpoint("pod-a", nil)

	runTurn(t, a, "s1", ep, 400, 900)
	idleFor(a, "s1", time.Minute)
	primeFitView(a, dlEp)

	// Admit s2 (estimate 300 > room 100), pausing s1.
	q2 := makeQueue("s2", time.Now(), 1200)
	require.Equal(t, q2, pick(t, a, q2))
	require.True(t, isPaused(a, "s1"))
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(a.metrics.closes.WithLabelValues(closeResultOK)) == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"s1"}, srv.sessions())

	// Evict s2's unconfirmed reservation so pod-a has room, then resume s1.
	a.mgr.mu.Lock()
	a.mgr.sessions["s2"].reservedUntil = time.Now().Add(-time.Second)
	a.mgr.mu.Unlock()
	primeFitView(a, dlEp)

	q1 := makeQueue("s1", time.Now(), 400)
	require.Equal(t, q1, pick(t, a, q1))
	runTurn(t, a, "s1", ep, 400, 900)
	require.False(t, isPaused(a, "s1"))
	require.Equal(t, []string{"s1"}, srv.sessions())
	require.Equal(t, float64(1), testutil.ToFloat64(a.metrics.closes.WithLabelValues(closeResultOK)))

	// Pause s1 again; moving or TTL-sweeping an already-paused session must
	// not send a duplicate close to pod-a.
	idleFor(a, "s1", time.Minute)
	primeFitView(a, dlEp)
	require.Equal(t, q2, pick(t, a, q2))
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(a.metrics.closes.WithLabelValues(closeResultOK)) == 2
	}, 2*time.Second, 10*time.Millisecond)
	a.mgr.mu.Lock()
	a.mgr.sessions["s1"].lastActivity = time.Now().Add(-2 * a.mgr.ttl)
	a.mgr.mu.Unlock()
	a.mgr.sweep(time.Now())
	require.Equal(t, []string{"s1", "s1"}, srv.sessions())
}

// TTL eviction in sweep sends POST /close_session for sessions that dispatched
// at least one turn, and skips never-dispatched reservations.
func TestCloseSessionOnSweepAndSkipNeverDispatched(t *testing.T) {
	srv := newCloseServer(t)
	a := newCloseTestAgent(t, closeTestConfig())
	ep := srv.schedEndpoint("pod-a", nil)
	dlEp := srv.dlEndpoint("pod-a", nil)

	runTurn(t, a, "dispatched", ep, 400, 300)
	primeFitView(a, dlEp)

	// Create a reservation in Pick that never reaches PreRequest.
	qReserved := makeQueue("reserved-only", time.Now(), 400)
	require.Equal(t, qReserved, pick(t, a, qReserved))

	pastTTL := time.Now().Add(-2 * a.mgr.ttl)
	a.mgr.mu.Lock()
	a.mgr.sessions["dispatched"].lastActivity = pastTTL
	a.mgr.sessions["reserved-only"].reservedUntil = pastTTL
	a.mgr.sessions["reserved-only"].lastActivity = pastTTL
	a.mgr.mu.Unlock()

	a.mgr.sweep(time.Now())
	_, okDispatched := sessionOf(a, "dispatched")
	_, okReserved := sessionOf(a, "reserved-only")
	require.False(t, okDispatched)
	require.False(t, okReserved)

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(a.metrics.closes.WithLabelValues(closeResultOK)) == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"dispatched"}, srv.sessions())
}

// When an already-dispatched session moves to another pod in PreRequest,
// POST /close_session is sent to the previous pod; a reservation that never
// dispatched on the reserved pod does not send a close when bound elsewhere.
func TestCloseSessionOnMove(t *testing.T) {
	srvA := newCloseServer(t)
	srvB := newCloseServer(t)
	a := newCloseTestAgent(t, closeTestConfig())
	epA := srvA.schedEndpoint("pod-a", nil)
	epB := srvB.schedEndpoint("pod-b", nil)
	primeFitView(a, srvA.dlEndpoint("pod-a", nil))

	// Reserve s-reserved on pod-a in Pick (turnCount == 0), then bind to pod-b in PreRequest.
	qRes := makeQueue("s-reserved", time.Now(), 400)
	require.Equal(t, qRes, pick(t, a, qRes))
	runTurn(t, a, "s-reserved", epB, 400, 300)

	// Dispatch s1 on pod-a (turnCount == 1), then move it to pod-b.
	runTurn(t, a, "s1", epA, 400, 300)
	runTurn(t, a, "s1", epB, 400, 350)

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(a.metrics.closes.WithLabelValues(closeResultOK)) == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"s1"}, srvA.sessions())
	require.Empty(t, srvB.sessions())
}

// No POST /close_session is sent when an endpoint leaves the pool via
// Saturation stale cleanup or Extract delete event, or when its unbound
// sessions later move to another pod.
func TestNoCloseWhenEndpointLeavesPool(t *testing.T) {
	srvA := newCloseServer(t)
	srvB := newCloseServer(t)
	a := newCloseTestAgent(t, closeTestConfig())
	epA := srvA.schedEndpoint("pod-a", nil)
	epB := srvB.schedEndpoint("pod-b", nil)

	// 1. Stale endpoint cleanup in Saturation.
	runTurn(t, a, "s1", epA, 400, 300)
	a.mgr.mu.Lock()
	a.mgr.endpoints["default/pod-a"].updatedAt = time.Now().Add(-2 * endpointStaleAfter)
	a.mgr.mu.Unlock()
	primeFitView(a, srvB.dlEndpoint("pod-b", nil))
	runTurn(t, a, "s1", epB, 400, 300)

	// 2. Endpoint delete event in Extract, followed by Pick reservation on
	// pod-b and PreRequest placement on pod-a: neither sends a close.
	runTurn(t, a, "s2", epA, 400, 300)
	require.NoError(t, a.Extract(context.Background(), fwkdl.EndpointEvent{
		Type:     fwkdl.EventDelete,
		Endpoint: srvA.dlEndpoint("pod-a", nil),
	}))
	primeFitView(a, srvB.dlEndpoint("pod-b", nil))
	q2 := makeQueue("s2", time.Now(), 400)
	require.Equal(t, q2, pick(t, a, q2))
	runTurn(t, a, "s2", epA, 400, 300)

	// 3. vLLM-labeled endpoint skips close on pause, move, and sweep.
	vllmLabels := map[string]string{fwkplugins.EngineTypeLabelKey: "vllm"}
	epVLLM := srvA.schedEndpoint("pod-vllm", vllmLabels)
	runTurn(t, a, "s-vllm", epVLLM, 400, 900)
	idleFor(a, "s-vllm", time.Minute)
	primeFitView(a, srvA.dlEndpoint("pod-vllm", vllmLabels))
	qNew := makeQueue("s-new", time.Now(), 1200)
	require.Equal(t, qNew, pick(t, a, qNew))
	runTurn(t, a, "s-vllm", epB, 400, 300)

	require.Empty(t, srvA.sessions())
	require.Empty(t, srvB.sessions())
	require.Equal(t, float64(0), testutil.ToFloat64(a.metrics.closes.WithLabelValues(closeResultOK)))
}

// Non-2xx responses and full close queues increment the error and dropped
// counters without blocking, and debug logs never contain session ids.
func TestCloseSessionErrorAndDroppedMetrics(t *testing.T) {
	var logMu sync.Mutex
	var logLines []string
	ctx, cancel := context.WithCancel(log.IntoContext(context.Background(), funcr.New(func(_, args string) {
		logMu.Lock()
		logLines = append(logLines, args)
		logMu.Unlock()
	}, funcr.Options{Verbosity: logutil.DEBUG})))
	defer cancel()

	srv := newCloseServer(t)
	srv.status = http.StatusInternalServerError
	p, err := Factory("thunder", json.NewDecoder(strings.NewReader(`{"capacityTokens": 1000, "closeSession": true}`)), utils.NewTestHandle(ctx))
	require.NoError(t, err)
	a := p.(*ThunderAgent)
	t.Cleanup(a.closer.close)

	epA := srv.schedEndpoint("pod-a", nil)
	epB := schedEndpoint("pod-b", 0, 0)

	runTurn(t, a, "secret-s1", epA, 400, 300)
	runTurn(t, a, "secret-s1", epB, 400, 300)

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(a.metrics.closes.WithLabelValues(closeResultError)) == 1
	}, 2*time.Second, 10*time.Millisecond)
	logMu.Lock()
	joinedLogs := strings.Join(logLines, "\n")
	logMu.Unlock()
	require.Contains(t, joinedLogs, "rejected session close")
	require.NotContains(t, joinedLogs, "secret-s1")

	// Stop workers so the buffered channel stays full, then verify a full
	// queue drops without blocking under lock.
	a.closer.close()
	for range closeQueueSize {
		a.closer.enqueue(closeJob{sessionID: "fill", addr: "127.0.0.1:8000"})
	}
	require.Equal(t, float64(0), testutil.ToFloat64(a.metrics.closes.WithLabelValues(closeResultDropped)))

	runTurn(t, a, "s2", epA, 400, 300)
	runTurn(t, a, "s2", epB, 400, 300)
	require.Equal(t, float64(1), testutil.ToFloat64(a.metrics.closes.WithLabelValues(closeResultDropped)))
}

// When two sessions are both past idleLeaseSeconds and the older-LRU session
// (s1) has a turn queued in the router, Pick skips s1 (!queued[s1]) and pauses
// the newer-LRU session (s2), sending POST /close_session only for s2 so
// SGLang's two-pass radix cache evicts s2 (session_ref=0) before s1 (session_ref=1).
func TestCloseSessionQueuedTurnInversion(t *testing.T) {
	srv := newCloseServer(t)
	a := newCloseTestAgent(t, closeTestConfig())
	ep := srv.schedEndpoint("pod-a", nil)
	dlEp := srv.dlEndpoint("pod-a", nil)

	// s1 finishes earlier (older in SGLang LRU), s2 finishes later; both exceed idleLeaseSeconds.
	runTurn(t, a, "s1", ep, 400, 450)
	runTurn(t, a, "s2", ep, 400, 450)
	idleFor(a, "s1", 2*time.Minute)
	idleFor(a, "s2", time.Minute)
	primeFitView(a, dlEp)

	// s1's next turn (650 tokens, pushing working set 650+450=1100 > 1000) is queued.
	q1 := makeQueue("s1", time.Now(), 2600)
	require.Equal(t, q1, pick(t, a, q1))
	require.False(t, isPaused(a, "s1"))
	require.True(t, isPaused(a, "s2"))

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(a.metrics.closes.WithLabelValues(closeResultOK)) == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"s2"}, srv.sessions())
}
