# ThunderAgent SGLang `close_session` 验证报告与代码

> [!IMPORTANT]
> **如何组织验证代码最优雅？**
> 1. **入库到 PR 的部分（符合 Go 与 `AGENTS.md` 规范）**：将核心验证逻辑（包括 SGLang HTTP mock Server、5 类生命周期触发与边界拦截、以及 **Queued-Turn Inversion** 场景）作为标准 Go 单元测试合入 [`pkg/epp/framework/plugins/thunderagent/closer_test.go`](file:///usr/local/google/home/bobzetian/projects/playground/llm-d-router/pkg/epp/framework/plugins/thunderagent/closer_test.go)，并将验证矩阵与实测输出写入 **PR #16 Description**，这样每次 CI `go test -race` 都会自动执行，且不向主干引入临时脚本文件。
> 2. **本地独立复现脚本与报告（供随时 Review 与交互运行）**：
>    - 独立端到端验证脚本（含 SGLang 两阶段 `session_ref` 驱逐模拟器与 A/B 对比）：[`docs/proposals/thunder-agent/verify_e2e.go`](file:///usr/local/google/home/bobzetian/projects/playground/llm-d-router/docs/proposals/thunder-agent/verify_e2e.go)
>    - 本地工作区副本：[`docs/proposals/thunder-agent/verification-report.md`](file:///usr/local/google/home/bobzetian/projects/playground/llm-d-router/docs/proposals/thunder-agent/verification-report.md)

[TOC]

---

## 1. 一键复现命令

```bash
# 1. 运行 PR 内的完整单元测试套件（含竞态检测，共 51 项测试）
cd /usr/local/google/home/bobzetian/projects/playground/llm-d-router && \
GO111MODULE=on go test -v -race ./pkg/epp/framework/plugins/thunderagent/... -run "TestPreRequestInjectsSessionID|TestCloseSession|TestNoClose"

# 2. 运行端到端 SGLang 两阶段 RadixCache 模拟器 + A/B 对比脚本
cd /usr/local/google/home/bobzetian/projects/playground/llm-d-router && \
GO111MODULE=on go run ./docs/proposals/thunder-agent/verify_e2e.go
```

---

## 2. 验证矩阵（Unit + E2E）

| 验证维度 | 对应测试函数 / 步骤 | 验证内容与断言 | 结果 |
| :--- | :--- | :--- | :--- |
| **1. 请求体 `session_id` 注入** | [`TestPreRequestInjectsSessionID`](file:///usr/local/google/home/bobzetian/projects/playground/llm-d-router/pkg/epp/framework/plugins/thunderagent/closer_test.go#L132-L259) | 覆盖 `/v1/chat/completions`、`/v1/completions`、SGLang `/generate`；覆盖客户端伪造的 `session_id`；当存在 `session_params`、目标为 `/v1/responses`、目标为 vLLM `/inference/v1/generate`（仅含 `token_ids`）、Pod 标记为 `vllm`、或 `closeSession=false` 时全部跳过 | `PASS` |
| **2. Pause 触发 `/close_session`（防重复）** | [`TestCloseSessionOnPauseAndNotOnResume`](file:///usr/local/google/home/bobzetian/projects/playground/llm-d-router/pkg/epp/framework/plugins/thunderagent/closer_test.go#L263-L307) | `Pick` 在 `reclaimLocked` 中 pause 空闲会话时异步发送 `POST /close_session`；同 Pod 恢复（resume）不发 close；已 pause 会话后续触发 TTL `sweep` 不重复发第二次 close | `PASS` |
| **3. TTL 过期触发 `/close_session`** | [`TestCloseSessionOnSweepAndSkipNeverDispatched`](file:///usr/local/google/home/bobzetian/projects/playground/llm-d-router/pkg/epp/framework/plugins/thunderagent/closer_test.go#L311-L341) | `sweep` 对 `turnCount > 0 && !paused` 的过期会话发送 `POST /close_session`；对仅在 `Pick` 预留但从未到达 `PreRequest`（`turnCount == 0`）的会话跳过 | `PASS` |
| **4. 跨 Pod 迁移触发 `/close_session`** | [`TestCloseSessionOnMove`](file:///usr/local/google/home/bobzetian/projects/playground/llm-d-router/pkg/epp/framework/plugins/thunderagent/closer_test.go#L346-L368) | 会话从 `pod-a` 迁移到 `pod-b` 时向 `pod-a` 发送 `POST /close_session`；仅预留未实际调度的会话改绑时不发送 | `PASS` |
| **5. Pod 退池与 vLLM 节点不发 close** | [`TestNoCloseWhenEndpointLeavesPool`](file:///usr/local/google/home/bobzetian/projects/playground/llm-d-router/pkg/epp/framework/plugins/thunderagent/closer_test.go#L373-L411) | `Saturation` 过期清理或 `Extract(EventDelete)` 解绑并重置 `paused`/`turnCount`，后续重新预留或调度到新 Pod 均不发 close；`vllm` 标签节点在 pause/move/sweep 下均不发 close | `PASS` |
| **6. 队列满丢弃、错误计数与日志脱敏** | [`TestCloseSessionErrorAndDroppedMetrics`](file:///usr/local/google/home/bobzetian/projects/playground/llm-d-router/pkg/epp/framework/plugins/thunderagent/closer_test.go#L415-L458) | HTTP 500 计入 `result="error"` 且 `V(DEBUG)` 日志不含 `session_id`；队列满（4096）非阻塞丢弃并计入 `result="dropped"` | `PASS` |
| **7. Queued-Turn Inversion（核心收益验证）** | [`TestCloseSessionQueuedTurnInversion`](file:///usr/local/google/home/bobzetian/projects/playground/llm-d-router/pkg/epp/framework/plugins/thunderagent/closer_test.go#L464-L487) & `verify_e2e.go` Step 5 | `s1` 在 SGLang LRU 中比 `s2` 更老，但 `s1` 的下一轮请求已在 Router 排队（`!queued[s1]`）：Router 跳过 `s1` 并 pause `s2`，向 SGLang 发送 `/close_session(s2)`，使 SGLang Pass 1 驱逐 `s2`（`session_ref=0`）而 100% 保留 `s1`（`session_ref=1`） | `PASS` |

---

## 3. 端到端实测输出（`verify_e2e.go`）

```text
=== [Step 1] Dispatch sess-A (600 tok) & sess-B (350 tok) to pod-1 (cap=1000) ===
  Outgoing wire JSON for sess-A: {"messages":[{"content":"hello","role":"user"}],"mock_tokens":600,"model":"glm-5","session_id":"sess-A"}
  pod-1 radix cache before pause: sess-A(ref=1, tok=600), sess-B(ref=1, tok=350)

=== [Step 2] New sess-C (500 tok) arrives -> Pick pauses idle sess-A -> POST /close_session ===
  pod-1 received POST /close_session for: [sess-A]
  pod-1 two-pass eviction result: evicted=[sess-A (pass=1)]
  pod-1 surviving cache entries: sess-C(ref=1, tok=500) sess-B(ref=1, tok=350) 

=== [Step 3] Active sess-B moves from pod-1 to pod-2 -> POST /close_session to pod-1 ===
  pod-1 received POST /close_session for: [sess-A sess-B]
  pod-2 cache entry for sess-B: ref=1, tok=400

=== [Step 4] Wait for TTL sweep (80ms) -> POST /close_session on expiry (no duplicate for paused sess-A) ===
  pod-1 total /close_session calls: [sess-A sess-B sess-C] (sess-A paused once, sess-B moved once, sess-C TTL-evicted once; NO duplicate for sess-A!)
  pod-2 total /close_session calls: [sess-B] (sess-B TTL-evicted once)

=== [Step 5] A/B Comparison: Queued-Turn Inversion (closeSession=false vs closeSession=true) ===
  [closeSession=false] /close_session=[] -> SGLang evicted=[sess-A (pass=1)] -> queued sess-A prefix survived=false
  [closeSession=true] /close_session=[sess-B] -> SGLang evicted=[sess-B (pass=1)] -> queued sess-A prefix survived=true
```

---

## 4. 端到端验证代码（`verify_e2e.go`）

<details>
<summary>点击展开完整可运行源码（<code>verify_e2e.go</code>）</summary>

```go
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
```
</details>
