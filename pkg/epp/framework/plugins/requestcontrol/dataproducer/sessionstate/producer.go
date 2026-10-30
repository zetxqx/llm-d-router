/*
Copyright 2026 The Kubernetes Authors.

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

// Package sessionstate provides a plugin that tracks agentic session state in
// a SessionStateRegistry and publishes it to request and endpoint attributes
// for session-aware flow-control, admission and scheduling plugins.
package sessionstate

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	sourcenotifications "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/notifications"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/requestheader/agentidentity"
)

const (
	defaultEvictionTTL           = time.Hour
	defaultEvictionSweepInterval = 5 * time.Minute
)

// inFlightTokensKey is the request attribute under which PreRequest stores the
// tokens it charged to the session, so ResponseBody releases exactly that
// amount.
var inFlightTokensKey = fwkplugin.NewDataKey("session-inflight-tokens", SessionStateProducerType)

// Parameters configures idle session eviction.
type Parameters struct {
	// EvictionTTLSeconds is the maximum idle time before session state is removed.
	// Zero disables eviction.
	EvictionTTLSeconds float64 `json:"evictionTtlSeconds,omitempty"`
	// EvictionSweepSeconds is how often idle session state is scanned.
	EvictionSweepSeconds float64 `json:"evictionSweepSeconds,omitempty"`
}

func (p Parameters) validate() error {
	if p.EvictionTTLSeconds < 0 {
		return fmt.Errorf("evictionTtlSeconds must be >= 0, got %v", p.EvictionTTLSeconds)
	}
	if p.EvictionSweepSeconds <= 0 {
		return fmt.Errorf("evictionSweepSeconds must be > 0, got %v", p.EvictionSweepSeconds)
	}
	return nil
}

var (
	_ requestcontrol.RequestHeaderProcessor = &Producer{}
	_ requestcontrol.PreRequest             = &Producer{}
	_ requestcontrol.ResponseBodyProcessor  = &Producer{}
	_ fwkplugin.ProducerPlugin              = &Producer{}
	_ fwkplugin.ConsumerPlugin              = &Producer{}
	_ fwkdl.Registrant                      = &Producer{}
	_ fwkdl.EndpointExtractor               = &Producer{}
)

// Producer tracks session state within one EPP instance.
type Producer struct {
	typedName  fwkplugin.TypedName
	dk         fwkplugin.DataKey
	endpointDK fwkplugin.DataKey

	registry SessionStateRegistry

	// registeredEndpoints maps endpoint IDs to the Endpoint last added, so a
	// delete for a replaced endpoint is ignored.
	registeredEndpoints sync.Map

	evictionTTL           time.Duration
	evictionSweepInterval time.Duration
}

// Factory builds a session-state producer.
func Factory(name string, rawParameters *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	params := Parameters{
		EvictionTTLSeconds:   defaultEvictionTTL.Seconds(),
		EvictionSweepSeconds: defaultEvictionSweepInterval.Seconds(),
	}
	if rawParameters != nil {
		if err := rawParameters.Decode(&params); err != nil {
			return nil, fmt.Errorf("invalid config for %s plugin %q: %w", SessionStateProducerType, name, err)
		}
	}
	if err := params.validate(); err != nil {
		return nil, fmt.Errorf("%s plugin %q: %w", SessionStateProducerType, name, err)
	}

	p := &Producer{
		typedName:             fwkplugin.TypedName{Type: SessionStateProducerType, Name: name},
		dk:                    SessionStateDataKey.WithNonEmptyProducerName(name),
		endpointDK:            EndpointSessionDataKey.WithNonEmptyProducerName(name),
		evictionTTL:           time.Duration(params.EvictionTTLSeconds * float64(time.Second)),
		evictionSweepInterval: time.Duration(params.EvictionSweepSeconds * float64(time.Second)),
	}
	if handle != nil && p.evictionTTL > 0 {
		go p.runEviction(handle.Context())
	}
	return p, nil
}

// LookupProducer returns the default session-state-producer instance. Plugins
// that read endpoint-level session state outside a request, such as
// flow-control policies, use its Registry after configuration loading has
// created the producer.
func LookupProducer(handle fwkplugin.HandlePlugins) (*Producer, error) {
	if handle == nil {
		return nil, fmt.Errorf("no plugin handle to look up %q", SessionStateProducerType)
	}
	return fwkplugin.PluginByType[*Producer](handle, SessionStateProducerType)
}

// TypedName returns the type and name of the plugin.
func (p *Producer) TypedName() fwkplugin.TypedName {
	return p.typedName
}

// Registry returns the registry this producer maintains.
func (p *Producer) Registry() *SessionStateRegistry {
	return &p.registry
}

// EvictionTTL returns how long an idle session is retained; 0 means forever.
func (p *Producer) EvictionTTL() time.Duration {
	return p.evictionTTL
}

// Produces declares the SessionState request attribute, the
// EndpointSessionSummary endpoint attribute, and the request attribute
// PreRequest writes for ResponseBody.
func (p *Producer) Produces() map[fwkplugin.DataKey]any {
	return map[fwkplugin.DataKey]any{
		p.dk:              SessionState{},
		p.endpointDK:      EndpointSessionSummary{},
		inFlightTokensKey: int64(0),
	}
}

// Consumes declares agent identity as a required input. The agent-identity
// plugin is intentionally not a default producer, so operators must enable an
// identity provider explicitly rather than having one auto-created.
func (p *Producer) Consumes() fwkplugin.DataDependencies {
	return fwkplugin.DataDependencies{
		Required: map[fwkplugin.DataKey]any{agentidentity.AgentIdentityKey: ""},
	}
}

// RequestHeader marks the session as seen and attaches a dynamic SessionState
// attribute before flow control, so every later read returns the session's
// current state, including while the request waits in a flow-control queue.
func (p *Producer) RequestHeader(_ context.Context, request *fwksched.InferenceRequest) error {
	identity, ok := readAgentIdentity(request)
	if !ok {
		return nil
	}

	record := p.registry.observe(identity)
	request.PutAttribute(p.dk, &fwksched.DynamicRequestAttribute{
		Get: func() any {
			state, ok := p.registry.resolve(identity, record)
			if !ok {
				return nil
			}
			return state
		},
	})
	return nil
}

// PreRequest records one dispatched turn on the primary profile's endpoint. A
// request counts once even when several profiles run.
func (p *Producer) PreRequest(_ context.Context, request *fwksched.InferenceRequest, result *fwksched.SchedulingResult) error {
	identity, ok := readAgentIdentity(request)
	if !ok {
		return nil
	}

	tokens := promptTokens(request)
	p.registry.RecordDispatch(identity, endpointID(result.PrimaryEndpoint()), tokens)
	request.PutAttribute(inFlightTokensKey, tokens)
	return nil
}

// ResponseBody settles a turn after its response lifecycle ends. Only
// naturally completed responses contribute completion and token totals.
func (p *Producer) ResponseBody(
	_ context.Context,
	request *fwksched.InferenceRequest,
	response *requestcontrol.Response,
	_ *fwkdl.EndpointMetadata,
) {
	if response == nil || !response.EndOfStream {
		return
	}
	identity, ok := readAgentIdentity(request)
	if !ok {
		return
	}
	// A request PreRequest did not charge has no turn to settle.
	charged, ok := fwksched.ReadRequestAttribute[int64](request, inFlightTokensKey)
	if !ok {
		return
	}

	p.registry.RecordResponse(identity, TurnOutcome{
		NaturallyCompleted: response.TerminationCause == requestcontrol.TerminationCauseNatural,
		InputTokens:        int64(response.Usage.PromptTokens),
		OutputTokens:       int64(response.Usage.CompletionTokens),
		TotalTokens:        int64(response.Usage.TotalTokens),
		InFlightTokens:     charged,
		FinishReason:       response.FinishReason,
		HasToolCalls:       response.HasToolCalls,
	})
}

// RegisterDependencies subscribes the producer to endpoint lifecycle events.
func (p *Producer) RegisterDependencies(r fwkdl.Registrar) error {
	return r.Register(fwkdl.PendingRegistration{
		Owner:      p.typedName,
		SourceType: sourcenotifications.EndpointNotificationSourceType,
		Extractor:  p,
		DefaultSource: sourcenotifications.NewEndpointDataSource(
			sourcenotifications.EndpointNotificationSourceType,
			sourcenotifications.EndpointNotificationSourceType,
		),
	})
}

// Extract registers endpoints with the registry and publishes their
// EndpointSessionSummary, and unbinds sessions from endpoints that leave the
// pool.
func (p *Producer) Extract(ctx context.Context, event fwkdl.EndpointEvent) error {
	if event.Endpoint == nil || event.Endpoint.GetMetadata() == nil {
		return nil
	}
	id := event.Endpoint.GetMetadata().ID.String()

	switch event.Type {
	case fwkdl.EventDelete:
		if registered, ok := p.registeredEndpoints.Load(id); ok && registered != event.Endpoint {
			log.FromContext(ctx).V(logutil.DEFAULT).Info("Ignoring stale delete for replaced endpoint", "endpoint", id)
			return nil
		}
		p.registeredEndpoints.Delete(id)
		p.registry.RemoveEndpoint(id)
	case fwkdl.EventAddOrUpdate:
		endpoint := event.Endpoint
		p.registeredEndpoints.Store(id, endpoint)
		p.registry.UpsertEndpoint(id, func() int64 { return capacityTokens(endpoint.GetMetrics()) })
		endpoint.GetAttributes().Put(p.endpointDK, &fwkdl.DynamicAttribute{
			Get: func() fwkdl.Cloneable {
				summary, ok := p.registry.EndpointSummary(id)
				if !ok {
					return nil
				}
				return &summary
			},
		})
	}
	return nil
}

func readAgentIdentity(request *fwksched.InferenceRequest) (string, bool) {
	if request == nil {
		return "", false
	}
	identity, ok := fwksched.ReadRequestAttribute[string](request, agentidentity.AgentIdentityKey)
	return identity, ok && identity != ""
}

// promptTokens returns the turn's full prompt length: the tokenized count when
// a token producer ran, else an estimate from the body size. It is at least 1,
// so InFlightTokens is nonzero while a turn is in flight.
func promptTokens(request *fwksched.InferenceRequest) int64 {
	if request.Body != nil && request.Body.TokenizedRequest != nil {
		if n := request.Body.TokenizedRequest.TokenCount(); n > 0 {
			return int64(n)
		}
	}
	return max(int64(request.RequestSizeBytes/fwkrh.BytesPerToken), 1)
}

func endpointID(endpoint fwksched.Endpoint) string {
	if endpoint == nil {
		return ""
	}
	md := endpoint.GetMetadata()
	if md == nil || md.ID.Name == "" {
		return ""
	}
	return md.ID.String()
}

// capacityTokens returns an endpoint's KV cache capacity in tokens, or 0 when
// the endpoint does not report its cache configuration.
func capacityTokens(m *fwkdl.Metrics) int64 {
	if m == nil || m.CacheBlockSize <= 0 || m.CacheNumBlocks <= 0 {
		return 0
	}
	return int64(m.CacheBlockSize) * int64(m.CacheNumBlocks)
}

func (p *Producer) runEviction(ctx context.Context) {
	ticker := time.NewTicker(p.evictionSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.evictIdle(time.Now())
		}
	}
}

func (p *Producer) evictIdle(now time.Time) {
	p.registry.EvictIdle(now, p.evictionTTL)
}
