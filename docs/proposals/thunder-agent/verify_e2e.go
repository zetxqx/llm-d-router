//go:build ignore

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkfc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwkfcmocks "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol/mocks"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/requestheader/agentidentity"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/openai"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/thunderagent"
)

type kvEntry struct {
	tokens     int
	sessionRef int
	lastAccess int
}

type mockSGLangPod struct {
	name     string
	capacity int
	srv      *httptest.Server
	host     string
	port     string

	mu         sync.Mutex
	clock      int
	cache      map[string]*kvEntry
	closeCalls []string
	evicted    []string
}

func newMockSGLangPod(name string, capacity int) *mockSGLangPod {
	p := &mockSGLangPod{
		name:     name,
		capacity: capacity,
		cache:    make(map[string]*kvEntry),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var payload map[string]any
		_ = json.Unmarshal(raw, &payload)
		sid, _ := payload["session_id"].(string)
		tokens := int(payload["mock_tokens"].(float64))
		if sid == "" {
			sid, _ = payload["flow_id"].(string)
			p.mu.Lock()
			p.clock++
			p.evictIfNeededLocked(sid, tokens)
			p.cache[sid] = &kvEntry{tokens: tokens, sessionRef: 0, lastAccess: p.clock}
			p.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}

		p.mu.Lock()
		p.clock++
		p.evictIfNeededLocked(sid, tokens)
		p.cache[sid] = &kvEntry{
			tokens:     tokens,
			sessionRef: 1,
			lastAccess: p.clock,
		}
		p.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/close_session", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body struct {
			SessionID string `json:"session_id"`
		}
		_ = json.Unmarshal(raw, &body)
		p.mu.Lock()
		p.closeCalls = append(p.closeCalls, body.SessionID)
		if entry, ok := p.cache[body.SessionID]; ok {
			entry.sessionRef = 0
		}
		p.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	p.srv = httptest.NewServer(mux)
	u, _ := url.Parse(p.srv.URL)
	p.host, p.port, _ = net.SplitHostPort(u.Host)
	return p
}

// evictIfNeededLocked implements SGLang's two-pass RadixCache eviction:
// Pass 1 evicts entries with sessionRef == 0 (LRU order); Pass 2 evicts sessionRef > 0.
func (p *mockSGLangPod) evictIfNeededLocked(incomingID string, incomingTokens int) {
	used := 0
	for id, e := range p.cache {
		if id != incomingID {
			used += e.tokens
		}
	}
	for pass := 0; pass < 2 && used+incomingTokens > p.capacity; pass++ {
		for used+incomingTokens > p.capacity {
			victimID := ""
			victimClock := int(^uint(0) >> 1)
			for id, e := range p.cache {
				if id == incomingID {
					continue
				}
				if (pass == 0 && e.sessionRef == 0) || (pass == 1 && e.sessionRef > 0) {
					if e.lastAccess < victimClock {
						victimClock = e.lastAccess
						victimID = id
					}
				}
			}
			if victimID == "" {
				break
			}
			used -= p.cache[victimID].tokens
			delete(p.cache, victimID)
			p.evicted = append(p.evicted, fmt.Sprintf("%s (pass=%d)", victimID, pass+1))
		}
	}
}

func (p *mockSGLangPod) endpoint() (fwkdl.Endpoint, fwksched.Endpoint) {
	md := &fwkdl.EndpointMetadata{
		ID:      types.NamespacedName{Namespace: "default", Name: p.name},
		Address: p.host,
		Port:    p.port,
	}
	return fwkdl.NewEndpoint(md, &fwkdl.Metrics{}), fwksched.NewEndpoint(md, &fwkdl.Metrics{}, nil)
}

func (p *mockSGLangPod) snapshot() (closes []string, evicted []string, state map[string]kvEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	closes = append([]string(nil), p.closeCalls...)
	evicted = append([]string(nil), p.evicted...)
	state = make(map[string]kvEntry, len(p.cache))
	for k, v := range p.cache {
		state[k] = *v
	}
	return
}

func waitForCloseCount(pod *mockSGLangPod, want int) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		closes, _, _ := pod.snapshot()
		if len(closes) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pod1 := newMockSGLangPod("pod-1", 1000)
	defer pod1.srv.Close()
	pod2 := newMockSGLangPod("pod-2", 1000)
	defer pod2.srv.Close()

	dl1, sched1 := pod1.endpoint()
	_, sched2 := pod2.endpoint()

	handle := fwkplugin.NewEppHandle(ctx, nil)
	cfgJSON := `{
		"capacityTokens": 1000,
		"utilThreshold": 1.0,
		"idleLeaseSeconds": 0.02,
		"evictionTtlSeconds": 0.08,
		"evictionSweepSeconds": 0.02,
		"headWaitStarvationMs": 0,
		"closeSession": true
	}`
	rawPlugin, err := thunderagent.Factory("thunder", json.NewDecoder(strings.NewReader(cfgJSON)), handle)
	if err != nil {
		panic(err)
	}
	ta := rawPlugin.(*thunderagent.ThunderAgent)
	parser := openai.NewOpenAIParser()

	sendTurn := func(sessionID string, tokens int, targetPod *mockSGLangPod, schedEp fwksched.Endpoint) string {
		rawClientJSON, _ := json.Marshal(map[string]any{
			"model":       "glm-5",
			"messages":    []map[string]string{{"role": "user", "content": "hello"}},
			"mock_tokens": tokens,
		})
		headers := map[string]string{":path": "/v1/chat/completions"}
		parsed, err := parser.ParseRequest(ctx, rawClientJSON, headers)
		if err != nil {
			panic(err)
		}
		schedReq := &fwksched.InferenceRequest{
			RequestID:        "req-" + sessionID,
			Headers:          headers,
			Body:             parsed.Body,
			RequestSizeBytes: tokens * 4,
		}
		schedReq.PutAttribute(agentidentity.AgentIdentityKey, sessionID)

		schedRes := &fwksched.SchedulingResult{
			PrimaryProfileName: "default",
			ProfileResults: map[string]*fwksched.ProfileRunResult{
				"default": {TargetEndpoints: []fwksched.Endpoint{schedEp}},
			},
		}
		if err := ta.PreRequest(ctx, schedReq, schedRes); err != nil {
			panic(err)
		}
		// Repackage wire body the same way Director.repackage does after PreRequest:
		wire := rawClientJSON
		if parsed.Body.Mutated {
			wire, err = json.Marshal(parsed.Body.Payload)
			if err != nil {
				panic(err)
			}
		}
		// Forward the repackaged wire RawBody to the mock SGLang HTTP server:
		resp, err := http.Post(targetPod.srv.URL+"/v1/chat/completions", "application/json", bytes.NewReader(wire))
		if err != nil {
			panic(err)
		}
		_ = resp.Body.Close()

		// Complete turn in ThunderAgent.ResponseBody:
		ta.ResponseBody(ctx, schedReq, &fwkrc.Response{
			EndOfStream: true,
			Usage:       fwkrh.Usage{PromptTokens: tokens, TotalTokens: tokens},
		}, schedEp.GetMetadata())
		return string(wire)
	}

	makeQueue := func(sessionID string, tokens int) fwkfc.FlowQueueAccessor {
		sizeBytes := uint64(tokens * 4)
		ir := &fwksched.InferenceRequest{
			RequestID:        "req-" + sessionID,
			RequestSizeBytes: int(sizeBytes),
		}
		ir.PutAttribute(agentidentity.AgentIdentityKey, sessionID)
		fcReq := fwkfcmocks.NewMockFlowControlRequest(sizeBytes, "req-"+sessionID, fwkfc.FlowKey{ID: sessionID})
		fcReq.InferenceRequestV = ir
		return &fwkfcmocks.MockFlowQueueAccessor{
			LenV:     1,
			FlowKeyV: fwkfc.FlowKey{ID: sessionID},
			PeekV: &fwkfcmocks.MockQueueItemAccessor{
				EnqueueTimeV:     time.Now(),
				OriginalRequestV: fcReq,
			},
		}
	}

	fmt.Println("=== [Step 1] Dispatch sess-A (600 tok) & sess-B (350 tok) to pod-1 (cap=1000) ===")
	wireA := sendTurn("sess-A", 600, pod1, sched1)
	fmt.Printf("  Outgoing wire JSON for sess-A: %s\n", wireA)
	// Wait 30ms so sess-A exceeds idleLeaseSeconds (20ms), then dispatch sess-B so sess-B stays within lease:
	time.Sleep(30 * time.Millisecond)
	_ = sendTurn("sess-B", 350, pod1, sched1)
	_, _, state1 := pod1.snapshot()
	fmt.Printf("  pod-1 radix cache before pause: sess-A(ref=%d, tok=%d), sess-B(ref=%d, tok=%d)\n\n",
		state1["sess-A"].sessionRef, state1["sess-A"].tokens,
		state1["sess-B"].sessionRef, state1["sess-B"].tokens)

	fmt.Println("=== [Step 2] New sess-C (500 tok) arrives -> Pick pauses idle sess-A -> POST /close_session ===")
	ta.Saturation(ctx, []fwkdl.Endpoint{dl1})
	band := &fwkfcmocks.MockPriorityBandAccessor{
		IterateQueuesFunc: func(cb func(fwkfc.FlowQueueAccessor) bool) {
			cb(makeQueue("sess-C", 500))
		},
	}
	picked, err := ta.Pick(ctx, band)
	if err != nil || picked == nil {
		panic("expected sess-C to be picked after pausing sess-A")
	}
	waitForCloseCount(pod1, 1)
	_ = sendTurn("sess-C", 500, pod1, sched1)
	closes1, evicted1, state1After := pod1.snapshot()
	fmt.Printf("  pod-1 received POST /close_session for: %v\n", closes1)
	fmt.Printf("  pod-1 two-pass eviction result: evicted=%v\n", evicted1)
	fmt.Printf("  pod-1 surviving cache entries: ")
	for k, v := range state1After {
		fmt.Printf("%s(ref=%d, tok=%d) ", k, v.sessionRef, v.tokens)
	}
	fmt.Println("\n")

	fmt.Println("=== [Step 3] Active sess-B moves from pod-1 to pod-2 -> POST /close_session to pod-1 ===")
	_ = sendTurn("sess-B", 400, pod2, sched2)
	waitForCloseCount(pod1, 2)
	closes1AfterMove, _, _ := pod1.snapshot()
	_, _, state2 := pod2.snapshot()
	fmt.Printf("  pod-1 received POST /close_session for: %v\n", closes1AfterMove)
	fmt.Printf("  pod-2 cache entry for sess-B: ref=%d, tok=%d\n\n", state2["sess-B"].sessionRef, state2["sess-B"].tokens)

	fmt.Println("=== [Step 4] Wait for TTL sweep (80ms) -> POST /close_session on expiry (no duplicate for paused sess-A) ===")
	waitForCloseCount(pod2, 1)
	waitForCloseCount(pod1, 3)
	closes1Final, _, _ := pod1.snapshot()
	closes2Final, _, _ := pod2.snapshot()
	fmt.Printf("  pod-1 total /close_session calls: %v (sess-A paused once, sess-B moved once, sess-C TTL-evicted once; NO duplicate for sess-A!)\n", closes1Final)
	fmt.Printf("  pod-2 total /close_session calls: %v (sess-B TTL-evicted once)\n\n", closes2Final)

	fmt.Println("=== [Step 5] A/B Comparison: Queued-Turn Inversion (closeSession=false vs closeSession=true) ===")
	for _, enableClose := range []bool{false, true} {
		podAB := newMockSGLangPod("pod-ab", 1000)
		dlAB, schedAB := podAB.endpoint()
		hAB := fwkplugin.NewEppHandle(ctx, nil)
		cfgAB := fmt.Sprintf(`{"capacityTokens":1000,"utilThreshold":1.0,"idleLeaseSeconds":0.02,"evictionTtlSeconds":10,"evictionSweepSeconds":10,"headWaitStarvationMs":0,"closeSession":%v}`, enableClose)
		pAB, _ := thunderagent.Factory("thunder", json.NewDecoder(strings.NewReader(cfgAB)), hAB)
		taAB := pAB.(*thunderagent.ThunderAgent)

		dispatchAB := func(sid string, tokens int) {
			rawJSON, _ := json.Marshal(map[string]any{"model": "glm-5", "messages": []map[string]string{{"role": "user", "content": "hi"}}, "mock_tokens": tokens, "flow_id": sid})
			hdrs := map[string]string{":path": "/v1/chat/completions"}
			pr, _ := parser.ParseRequest(ctx, rawJSON, hdrs)
			sr := &fwksched.InferenceRequest{RequestID: "req-" + sid, Headers: hdrs, Body: pr.Body, RequestSizeBytes: tokens * 4}
			sr.PutAttribute(agentidentity.AgentIdentityKey, sid)
			sRes := &fwksched.SchedulingResult{PrimaryProfileName: "default", ProfileResults: map[string]*fwksched.ProfileRunResult{"default": {TargetEndpoints: []fwksched.Endpoint{schedAB}}}}
			_ = taAB.PreRequest(ctx, sr, sRes)
			wire := rawJSON
			if pr.Body.Mutated {
				wire, _ = json.Marshal(pr.Body.Payload)
			}
			resp, _ := http.Post(podAB.srv.URL+"/v1/chat/completions", "application/json", bytes.NewReader(wire))
			_ = resp.Body.Close()
			taAB.ResponseBody(ctx, sr, &fwkrc.Response{EndOfStream: true, Usage: fwkrh.Usage{PromptTokens: tokens, TotalTokens: tokens}}, schedAB.GetMetadata())
		}

		// t=1: sess-A finishes (450 tokens, older in SGLang LRU: lastAccess=1)
		dispatchAB("sess-A", 450)
		// t=2: sess-B finishes (450 tokens, newer in SGLang LRU: lastAccess=2)
		dispatchAB("sess-B", 450)
		// Both exceed idleLeaseSeconds (20ms):
		time.Sleep(30 * time.Millisecond)

		// Now sess-A has its next turn queued (growing from 450 -> 650 tokens, total 650+450=1100 > 1000)!
		// Pick dispatches sess-A (admitted) and reclaims idle sess-B (because sess-A is queued):
		taAB.Saturation(ctx, []fwkdl.Endpoint{dlAB})
		bandAB := &fwkfcmocks.MockPriorityBandAccessor{
			IterateQueuesFunc: func(cb func(fwkfc.FlowQueueAccessor) bool) {
				cb(makeQueue("sess-A", 650))
			},
		}
		_, _ = taAB.Pick(ctx, bandAB)
		if enableClose {
			waitForCloseCount(podAB, 1)
		}
		// Before sess-A's turn reaches the pod, an uncached burst sess-X (200 tokens) allocates on the pod (450+450+200 = 1100 > 1000):
		dispatchAB("sess-X", 200)
		closesAB, evictedAB, stateAB := podAB.snapshot()
		_, aSurvived := stateAB["sess-A"]
		fmt.Printf("  [closeSession=%v] /close_session=%v -> SGLang evicted=%v -> queued sess-A prefix survived=%v\n",
			enableClose, closesAB, evictedAB, aSurvived)
		podAB.srv.Close()
	}
}
