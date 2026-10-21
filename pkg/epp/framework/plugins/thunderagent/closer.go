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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwkplugins "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins"
)

const (
	closeQueueSize = 4096
	closeWorkers   = 4
	closeTimeout   = time.Second
	closePath      = "/close_session"

	closeResultOK      = "ok"
	closeResultError   = "error"
	closeResultDropped = "dropped"

	legacyEngineTypeLabelKey = "inference.networking.k8s.io/engine-type"
)

type closeJob struct {
	sessionID string
	addr      string
}

type closePayload struct {
	SessionID string `json:"session_id"`
}

// sessionCloser sends asynchronous POST /close_session requests to SGLang
// endpoints when sessions are paused, TTL-evicted, or moved.
type sessionCloser struct {
	queue   chan closeJob
	client  *http.Client
	metrics *thunderMetrics
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func newSessionCloser(parent context.Context, metrics *thunderMetrics) *sessionCloser {
	ctx, cancel := context.WithCancel(parent)
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = closeWorkers
	c := &sessionCloser{
		queue:   make(chan closeJob, closeQueueSize),
		client:  &http.Client{Timeout: closeTimeout, Transport: tr},
		metrics: metrics,
		cancel:  cancel,
	}
	c.wg.Add(closeWorkers)
	for range closeWorkers {
		go func() {
			defer c.wg.Done()
			c.runWorker(ctx)
		}()
	}
	return c
}

func (c *sessionCloser) enqueue(jobs ...closeJob) {
	if c == nil {
		return
	}
	for _, job := range jobs {
		if job.addr == "" || job.sessionID == "" {
			continue
		}
		select {
		case c.queue <- job:
		default:
			c.metrics.closes.WithLabelValues(closeResultDropped).Inc()
		}
	}
}

func (c *sessionCloser) close() {
	if c == nil {
		return
	}
	c.cancel()
	c.wg.Wait()
}

func (c *sessionCloser) runWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-c.queue:
			c.sendClose(ctx, job)
		}
	}
}

func (c *sessionCloser) sendClose(ctx context.Context, job closeJob) {
	body, err := json.Marshal(closePayload{SessionID: job.sessionID})
	if err != nil {
		c.metrics.closes.WithLabelValues(closeResultError).Inc()
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+job.addr+closePath, bytes.NewReader(body))
	if err != nil {
		c.metrics.closes.WithLabelValues(closeResultError).Inc()
		log.FromContext(ctx).V(logutil.DEBUG).Info("Failed to build session close request", "endpoint", job.addr, "error", err)
		return
	}
	req.Header.Set(reqcommon.HeaderContentType, reqcommon.ContentTypeJSON)
	resp, err := c.client.Do(req)
	if err != nil {
		c.metrics.closes.WithLabelValues(closeResultError).Inc()
		log.FromContext(ctx).V(logutil.DEBUG).Info("Failed to close session on endpoint", "endpoint", job.addr, "error", err)
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		c.metrics.closes.WithLabelValues(closeResultError).Inc()
		log.FromContext(ctx).V(logutil.DEBUG).Info("Endpoint rejected session close", "endpoint", job.addr, "status", resp.StatusCode)
		return
	}
	c.metrics.closes.WithLabelValues(closeResultOK).Inc()
}

// isSGLangEndpoint reports whether md targets an SGLang engine. Unlabeled
// endpoints default to SGLang here so SGLang deployments need no extra label.
func isSGLangEndpoint(md *datalayer.EndpointMetadata) bool {
	if md == nil {
		return false
	}
	engine := md.Labels[fwkplugins.EngineTypeLabelKey]
	if engine == "" {
		engine = md.Labels[legacyEngineTypeLabelKey]
	}
	return engine == "" || strings.EqualFold(engine, "sglang")
}

func (a *ThunderAgent) closeAddr(md *datalayer.EndpointMetadata) string {
	if !a.closeSession || !isSGLangEndpoint(md) || md.Address == "" || md.Port == "" {
		return ""
	}
	return net.JoinHostPort(md.Address, md.Port)
}

func injectSessionID(body *fwkrh.InferenceRequestBody, id string) {
	if body == nil || (body.ChatCompletions == nil && body.Completions == nil && body.Generate == nil) {
		return
	}
	m, ok := body.Payload.(fwkrh.PayloadMap)
	if !ok || m == nil {
		return
	}
	if body.Generate != nil && m["input_ids"] == nil && m["text"] == nil {
		return
	}
	if _, hasParams := m["session_params"]; !hasParams {
		m["session_id"] = id
		body.Mutated = true
	}
}
