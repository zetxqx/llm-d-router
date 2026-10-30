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
	"errors"
	"fmt"
	"os"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
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

// Mode values.
const (
	ModeDogfood  = "dogfood"
	ModeCustomer = "customer"
)

const (
	defaultSampleRate        = 0.01
	defaultSinkPath          = "/var/weka-trace"
	defaultFileBytes         = 64 << 20
	defaultDiskHighWatermark = 900 << 20
	// queueBytes bounds pending work, including referenced token slices.
	queueBytes = 64 << 20
	// lruBytes bounds the per-session delta state.
	lruBytes = 64 << 20
	// customerArrivalResolutionMs coarsens arrival times in customer mode so
	// that joining records to trace backends by timestamp is less precise.
	customerArrivalResolutionMs = 100

	requestStateKey fwkplugin.StateKey = "weka-trace-recorder.request"
)

// Config is the plugin's parameter block. See README.md.
type Config struct {
	Mode                     string     `json:"mode"`
	Consent                  bool       `json:"consent"`
	KeySecretPath            string     `json:"keySecretPath"`
	KeyID                    string     `json:"keyID"`
	SampleRate               *float64   `json:"sampleRate,omitempty"`
	AdditionalSessionHeaders []string   `json:"additionalSessionHeaders,omitempty"`
	ModelAllowlist           []string   `json:"modelAllowlist,omitempty"`
	Sink                     SinkConfig `json:"sink"`
}

// SinkConfig configures the file sink.
type SinkConfig struct {
	Path              string             `json:"path,omitempty"`
	FileBytes         *resource.Quantity `json:"fileBytes,omitempty"`
	DiskHighWatermark *resource.Quantity `json:"diskHighWatermark,omitempty"`
	Stdout            bool               `json:"stdout,omitempty"`
}

// settings is Config after validation and defaulting.
type settings struct {
	keySecretPath       string
	keyID               string
	sampleRate          float64
	sessionHeaders      []string
	modelAllowlist      map[string]bool
	sinkPath            string
	fileBytes           int64
	diskHighWatermark   int64
	stdout              bool
	arrivalResolutionMs int64
}

// settings validates c. Errors fail config loading, so they are reserved for
// misconfiguration that must be visible at deploy time.
func (c Config) settings() (settings, error) {
	switch {
	case c.Mode != ModeDogfood && c.Mode != ModeCustomer:
		return settings{}, fmt.Errorf("mode must be %q or %q, got %q", ModeDogfood, ModeCustomer, c.Mode)
	case c.Mode == ModeCustomer && !c.Consent:
		return settings{}, errors.New("mode customer requires consent: true")
	case c.Mode == ModeCustomer && c.Sink.Stdout:
		return settings{}, errors.New("sink.stdout is not allowed in mode customer")
	case c.KeySecretPath == "":
		return settings{}, errors.New("keySecretPath is required")
	}
	s := settings{
		keySecretPath:     c.KeySecretPath,
		keyID:             c.KeyID,
		sampleRate:        defaultSampleRate,
		modelAllowlist:    map[string]bool{},
		sinkPath:          c.Sink.Path,
		fileBytes:         defaultFileBytes,
		diskHighWatermark: defaultDiskHighWatermark,
		stdout:            c.Sink.Stdout,
	}
	if c.SampleRate != nil {
		s.sampleRate = *c.SampleRate
	}
	if s.sampleRate < 0 || s.sampleRate > 1 {
		return settings{}, fmt.Errorf("sampleRate %v is outside [0, 1]", s.sampleRate)
	}
	if s.sinkPath == "" {
		s.sinkPath = defaultSinkPath
	}
	if c.Sink.FileBytes != nil {
		s.fileBytes = c.Sink.FileBytes.Value()
	}
	if c.Sink.DiskHighWatermark != nil {
		s.diskHighWatermark = c.Sink.DiskHighWatermark.Value()
	}
	if s.fileBytes <= 0 || s.diskHighWatermark <= 0 {
		return settings{}, errors.New("sink byte limits must be positive")
	}
	if c.Mode == ModeCustomer {
		s.arrivalResolutionMs = customerArrivalResolutionMs
	}
	for _, m := range c.ModelAllowlist {
		s.modelAllowlist[m] = true
	}
	for _, h := range c.AdditionalSessionHeaders {
		if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
			s.sessionHeaders = append(s.sessionHeaders, h)
		}
	}
	s.sessionHeaders = append(s.sessionHeaders,
		agentidentity.ClaudeCodeSessionHeader,
		agentidentity.OpenCodeSessionHeader,
		agentidentity.CodexSessionHeader,
		agentidentity.CodexSessionHeaderLegacy,
	)
	return s, nil
}

var (
	_ requestcontrol.PreRequest            = &Recorder{}
	_ requestcontrol.ResponseBodyProcessor = &Recorder{}
	_ fwkplugin.ConsumerPlugin             = &Recorder{}
	_ fwkplugin.StateDumper                = &Recorder{}
)

// Recorder implements the weka-trace-recorder plugin.
type Recorder struct {
	typedName fwkplugin.TypedName
	cfg       settings
	state     *fwkplugin.PluginState
	// worker is nil when the recorder disabled itself (for example, no key).
	worker *worker
	stats  *counters
}

// Factory builds a Recorder from raw parameters.
func Factory(name string, rawParameters *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	var cfg Config
	if rawParameters != nil {
		if err := rawParameters.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("failed to parse parameters for plugin %q: %w", name, err)
		}
	}
	s, err := cfg.settings()
	if err != nil {
		return nil, fmt.Errorf("invalid parameters for plugin %q: %w", name, err)
	}
	ctx, backend := context.Background(), func() string { return "unknown" }
	if handle != nil {
		ctx, backend = handle.Context(), func() string { return tokenBackend(handle) }
	}
	return newRecorder(ctx, name, s, backend), nil
}

// newRecorder disables the recorder, rather than failing, when the key or the
// sink is unavailable: recording must never affect requests.
func newRecorder(ctx context.Context, name string, s settings, backend func() string) *Recorder {
	r := &Recorder{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: name},
		cfg:       s,
		state:     fwkplugin.NewPluginState(ctx),
		stats:     &counters{},
	}
	logger := log.FromContext(ctx).WithValues("plugin", PluginType)
	key, err := os.ReadFile(s.keySecretPath)
	if err == nil && len(key) == 0 {
		err = fmt.Errorf("key file %q is empty", s.keySecretPath)
	}
	if err != nil {
		logger.Error(err, "Recorder disabled: PRF key unavailable")
		return r
	}
	sink, err := newFileSink(s, podName())
	if err != nil {
		logger.Error(err, "Recorder disabled: sink unavailable")
		return r
	}
	r.worker = newWorker(ctx, s, key, sink, r.stats, backend)
	logger.Info("Recorder enabled", "kid", s.keyID, "sampleRate", s.sampleRate, "path", s.sinkPath)
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

// requestState is what PreRequest hands to the end-of-stream hook. The request
// pointer is shared on purpose: its body is not mutated after scheduling.
type requestState struct {
	sessionID string
	request   *fwksched.InferenceRequest
	endpoint  string
	profile   string
}

// Clone implements fwkplugin.StateData.
func (s *requestState) Clone() fwkplugin.StateData {
	cp := *s
	return &cp
}

// PreRequest decides sampling and captures what the end-of-stream hook needs.
// It always returns nil: a returned error fails the request.
func (r *Recorder) PreRequest(ctx context.Context, request *fwksched.InferenceRequest, result *fwksched.SchedulingResult) error {
	defer r.recoverHook(ctx, "PreRequest")
	if r.worker == nil || request == nil || request.RequestID == "" {
		return nil
	}
	sid := ""
	for _, h := range r.cfg.sessionHeaders {
		if sid = strings.TrimSpace(request.Headers[h]); sid != "" {
			break
		}
	}
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

// ResponseBody enqueues a record at end of stream; other chunks return at once.
func (r *Recorder) ResponseBody(ctx context.Context, request *fwksched.InferenceRequest, response *requestcontrol.Response, _ *fwkdl.EndpointMetadata) {
	defer r.recoverHook(ctx, "ResponseBody")
	if response == nil || !response.EndOfStream || r.worker == nil || request == nil || request.RequestID == "" {
		return
	}
	st, err := fwkplugin.ReadPluginStateKey[*requestState](r.state, request.RequestID, requestStateKey)
	r.state.Delete(request.RequestID)
	if err == nil {
		r.worker.enqueue(st, response)
	}
}

func (r *Recorder) recoverHook(ctx context.Context, hook string) {
	if v := recover(); v != nil {
		r.stats.panics.Add(1)
		log.FromContext(ctx).Error(fmt.Errorf("%v", v), "Recorder hook panicked", "hook", hook)
	}
}

// DumpState exposes counters for the operational debug endpoint.
func (r *Recorder) DumpState() (json.RawMessage, error) {
	return json.Marshal(map[string]any{"enabled": r.worker != nil, "kid": r.cfg.keyID, "counters": r.stats})
}

// tokenBackend reports the configured token-producer backend, or "none".
func tokenBackend(handle fwkplugin.Handle) string {
	for _, p := range handle.GetAllPlugins() {
		if b, ok := p.(interface{ Backend() string }); ok && p.TypedName().Type == tokenizer.PluginType {
			return b.Backend()
		}
	}
	return "none"
}

func podName() string {
	if v := os.Getenv("POD_NAME"); v != "" {
		return v
	}
	h, _ := os.Hostname()
	return h
}
