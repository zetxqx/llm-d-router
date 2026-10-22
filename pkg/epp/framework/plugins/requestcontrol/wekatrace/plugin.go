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

// Package wekatrace provides the weka-trace-recorder plugin. It writes one
// content-free shape record per sampled request so that traffic can be
// exported offline as WekaTrace. See docs/proposals/weka-trace-recorder.
package wekatrace

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"sigs.k8s.io/controller-runtime/pkg/log"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/tokenizer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/requestheader/agentidentity"
)

// PluginType is the registered type name.
const PluginType = "weka-trace-recorder"

const requestStateKey fwkplugin.StateKey = "weka-trace-recorder.request"

var (
	_ requestcontrol.PreRequest            = &Recorder{}
	_ requestcontrol.ResponseBodyProcessor = &Recorder{}
	_ fwkplugin.ConsumerPlugin             = &Recorder{}
	_ fwkplugin.StateDumper                = &Recorder{}
)

// Recorder implements the weka-trace-recorder plugin.
type Recorder struct {
	typedName fwkplugin.TypedName
	cfg       resolvedConfig
	state     *fwkplugin.PluginState
	handle    fwkplugin.Handle
	// worker is nil when the recorder disabled itself (for example, no key).
	worker *worker
	stats  *stats
}

// Factory builds a Recorder from raw parameters.
func Factory(name string, rawParameters *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	var cfg Config
	if rawParameters != nil {
		if err := rawParameters.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("failed to parse parameters for plugin %q: %w", name, err)
		}
	}
	resolved, err := cfg.resolve()
	if err != nil {
		return nil, fmt.Errorf("invalid parameters for plugin %q: %w", name, err)
	}
	ctx := context.Background()
	if handle != nil {
		ctx = handle.Context()
	}
	return newRecorder(ctx, name, resolved, handle), nil
}

func newRecorder(ctx context.Context, name string, cfg resolvedConfig, handle fwkplugin.Handle) *Recorder {
	r := &Recorder{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: name},
		cfg:       cfg,
		state:     fwkplugin.NewPluginState(ctx),
		handle:    handle,
		stats:     &stats{},
	}
	logger := log.FromContext(ctx).WithValues("plugin", PluginType)
	key, err := os.ReadFile(cfg.keySecretPath)
	if err == nil && len(key) == 0 {
		err = fmt.Errorf("key file %q is empty", cfg.keySecretPath)
	}
	if err != nil {
		logger.Error(err, "Recorder disabled: PRF key unavailable")
		return r
	}
	sink, err := newFileSink(cfg, podName())
	if err != nil {
		logger.Error(err, "Recorder disabled: sink unavailable")
		return r
	}
	r.worker = newWorker(ctx, cfg, key, sink, r.stats, r.tokenBackend)
	logger.Info("Recorder enabled", "mode", cfg.mode, "kid", cfg.keyID, "sampleRate", cfg.sampleRate, "path", cfg.sinkPath)
	return r
}

// TypedName returns the plugin's typed name.
func (r *Recorder) TypedName() fwkplugin.TypedName { return r.typedName }

// Consumes requires tokenized prompts so that block hashes use real token ids.
func (r *Recorder) Consumes() fwkplugin.DataDependencies {
	return fwkplugin.DataDependencies{
		Required: map[fwkplugin.DataKey]any{tokenizer.TokenizedPromptDataKey: fwksched.TokenizedRequest{}},
	}
}

// requestState is what PreRequest hands to the end-of-stream hook.
type requestState struct {
	sessionID string
	request   *fwksched.InferenceRequest
	endpoint  string
	profile   string
}

// Clone implements fwkplugin.StateData. The request pointer is shared on
// purpose: its body is not mutated after scheduling.
func (s *requestState) Clone() fwkplugin.StateData {
	if s == nil {
		return nil
	}
	cp := *s
	return &cp
}

// PreRequest decides sampling and captures what the end-of-stream hook needs.
// It always returns nil: a returned error fails the request.
func (r *Recorder) PreRequest(ctx context.Context, request *fwksched.InferenceRequest, result *fwksched.SchedulingResult) (err error) {
	defer r.recoverHook(ctx, "PreRequest")
	if r.worker == nil || request == nil || request.RequestID == "" {
		return nil
	}
	sid := sessionIDFromHeaders(request.Headers, r.cfg.sessionHeaders)
	if !r.worker.sampled(sid, request.RequestID) {
		return nil
	}
	st := &requestState{sessionID: sid, request: request}
	if ep := result.PrimaryEndpoint(); ep != nil && ep.GetMetadata() != nil {
		st.endpoint = ep.GetMetadata().ID.String()
	}
	if result != nil {
		st.profile = result.PrimaryProfileName
	}
	r.state.Write(request.RequestID, requestStateKey, st)
	r.state.BindLiveness(ctx, request.RequestID)
	return nil
}

// ResponseBody enqueues a record at end of stream. Intermediate chunks return
// immediately.
func (r *Recorder) ResponseBody(ctx context.Context, request *fwksched.InferenceRequest, response *requestcontrol.Response, _ *fwkdl.EndpointMetadata) {
	defer r.recoverHook(ctx, "ResponseBody")
	if response == nil || !response.EndOfStream || r.worker == nil || request == nil || request.RequestID == "" {
		return
	}
	st, err := fwkplugin.ReadPluginStateKey[*requestState](r.state, request.RequestID, requestStateKey)
	r.state.Delete(request.RequestID)
	if err != nil || st == nil {
		return
	}
	r.worker.enqueue(newJob(st, response))
}

func (r *Recorder) recoverHook(ctx context.Context, hook string) {
	if v := recover(); v != nil {
		r.stats.panics.Add(1)
		log.FromContext(ctx).Error(fmt.Errorf("%v", v), "Recorder hook panicked", "hook", hook)
	}
}

// tokenBackend reports the configured token-producer backend, or "none".
func (r *Recorder) tokenBackend() string {
	if r.handle == nil {
		return "unknown"
	}
	for _, p := range r.handle.GetAllPlugins() {
		if b, ok := p.(interface{ Backend() string }); ok && p.TypedName().Type == tokenizer.PluginType {
			return b.Backend()
		}
	}
	return "none"
}

// DumpState exposes counters for the operational debug endpoint.
func (r *Recorder) DumpState() (json.RawMessage, error) {
	return json.Marshal(r.stats.snapshot(r.worker != nil, r.cfg.keyID))
}

// sessionHeaders returns the operator headers followed by the agent-identity
// defaults, lowercased.
func sessionHeaders(additional []string) []string {
	out := make([]string, 0, len(additional)+4)
	for _, h := range additional {
		if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
			out = append(out, h)
		}
	}
	return append(out,
		agentidentity.ClaudeCodeSessionHeader,
		agentidentity.OpenCodeSessionHeader,
		agentidentity.CodexSessionHeader,
		agentidentity.CodexSessionHeaderLegacy,
	)
}

func sessionIDFromHeaders(headers map[string]string, names []string) string {
	for _, h := range names {
		if v := strings.TrimSpace(headers[h]); v != "" {
			return v
		}
	}
	return ""
}

func podName() string {
	if v := os.Getenv("POD_NAME"); v != "" {
		return v
	}
	h, _ := os.Hostname()
	return h
}

// stats are process-local counters exposed through DumpState.
type stats struct {
	written      atomic.Int64
	droppedQueue atomic.Int64
	droppedSink  atomic.Int64
	droppedDisk  atomic.Int64
	keyframes    atomic.Int64
	panics       atomic.Int64
	workerPanics atomic.Int64
	queueBytes   atomic.Int64
	bytesWritten atomic.Int64
}

type statsSnapshot struct {
	Enabled      bool   `json:"enabled"`
	Kid          string `json:"kid"`
	Written      int64  `json:"written"`
	DroppedQueue int64  `json:"droppedQueue"`
	DroppedSink  int64  `json:"droppedSink"`
	DroppedDisk  int64  `json:"droppedDisk"`
	Keyframes    int64  `json:"keyframes"`
	Panics       int64  `json:"panics"`
	WorkerPanics int64  `json:"workerPanics"`
	QueueBytes   int64  `json:"queueBytes"`
	BytesWritten int64  `json:"bytesWritten"`
}

func (s *stats) snapshot(enabled bool, kid string) statsSnapshot {
	return statsSnapshot{
		Enabled: enabled, Kid: kid,
		Written: s.written.Load(), DroppedQueue: s.droppedQueue.Load(), DroppedSink: s.droppedSink.Load(),
		DroppedDisk: s.droppedDisk.Load(), Keyframes: s.keyframes.Load(), Panics: s.panics.Load(),
		WorkerPanics: s.workerPanics.Load(), QueueBytes: s.queueBytes.Load(), BytesWritten: s.bytesWritten.Load(),
	}
}
