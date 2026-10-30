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

package sessionstate

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	eppdatalayer "github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrsession "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/session"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/requestheader/agentidentity"
)

func newTestProducer(t *testing.T) *Producer {
	t.Helper()
	plg, err := Factory(SessionStateProducerType, nil, nil)
	require.NoError(t, err)
	producer, ok := plg.(*Producer)
	require.True(t, ok)
	return producer
}

func requestWithAgentIdentity(agentIdentity string) *fwksched.InferenceRequest {
	request := &fwksched.InferenceRequest{RequestSizeBytes: 400}
	request.PutAttribute(agentidentity.AgentIdentityKey, agentIdentity)
	return request
}

// podA is the scheduling endpoint default/pod-a.
func podA() fwksched.Endpoint {
	return fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: types.NamespacedName{Namespace: "default", Name: "pod-a"}}, fwkdl.NewMetrics(), fwkdl.NewAttributes())
}

func resultFor(endpoint fwksched.Endpoint) *fwksched.SchedulingResult {
	return &fwksched.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults: map[string]*fwksched.ProfileRunResult{
			"default": {TargetEndpoints: []fwksched.Endpoint{endpoint}},
		},
	}
}

func resultWithProfiles(profileCount int) *fwksched.SchedulingResult {
	profiles := make(map[string]*fwksched.ProfileRunResult, profileCount)
	for i := range profileCount {
		profiles[string(rune('a'+i))] = &fwksched.ProfileRunResult{
			TargetEndpoints: []fwksched.Endpoint{
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{}, fwkdl.NewMetrics(), fwkdl.NewAttributes()),
			},
		}
	}
	return &fwksched.SchedulingResult{ProfileResults: profiles, PrimaryProfileName: "a"}
}

func finalResponse(cause fwkrc.TerminationCause, inputTokens, outputTokens int) *fwkrc.Response {
	return &fwkrc.Response{
		EndOfStream:      true,
		TerminationCause: cause,
		Usage: fwkrh.Usage{
			PromptTokens:     inputTokens,
			CompletionTokens: outputTokens,
		},
	}
}

// startTurn runs the producer's request hooks for one dispatched turn of
// session-a.
func startTurn(t *testing.T, producer *Producer, result *fwksched.SchedulingResult) *fwksched.InferenceRequest {
	t.Helper()
	request := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.RequestHeader(context.Background(), request))
	require.NoError(t, producer.PreRequest(context.Background(), request, result))
	return request
}

// observed returns session-a's state as a new request reads it.
func observed(t *testing.T, producer *Producer) SessionState {
	t.Helper()
	request := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.RequestHeader(context.Background(), request))
	state, ok := ReadSessionState(request)
	require.True(t, ok)
	return state
}

func TestFactoryAndProduces(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)

	assert.Equal(t, fwkplugin.TypedName{Type: SessionStateProducerType, Name: SessionStateProducerType}, producer.TypedName())
	produced, ok := producer.Produces()[SessionStateDataKey.WithNonEmptyProducerName(SessionStateProducerType)]
	require.True(t, ok)
	assert.IsType(t, SessionState{}, produced)
	produced, ok = producer.Produces()[EndpointSessionDataKey.WithNonEmptyProducerName(SessionStateProducerType)]
	require.True(t, ok)
	assert.IsType(t, EndpointSessionSummary{}, produced)
	assert.Equal(t, defaultEvictionTTL, producer.evictionTTL)
	assert.Equal(t, defaultEvictionSweepInterval, producer.evictionSweepInterval)

	dependencies := producer.Consumes()
	consumed, ok := dependencies.Required[agentidentity.AgentIdentityKey]
	require.True(t, ok)
	assert.IsType(t, "", consumed)
	assert.Empty(t, dependencies.Optional)
}

func TestAgentIdentityIsARequiredDependency(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	handle := fwkplugin.NewEppHandle(context.Background(), nil)
	handle.AddPlugin(producer.TypedName().Name, producer)

	err := eppdatalayer.CreateMissingDataProducers(
		context.Background(),
		map[string]string{},
		map[string]fwkplugin.FactoryFunc{},
		handle,
	)
	require.ErrorIs(t, err, eppdatalayer.ErrNoDefaultProducer)
	assert.ErrorContains(t, err, agentidentity.AgentIdentityKey.String())
	assert.ErrorContains(t, err, producer.TypedName().Name)
}

func TestAgentIdentitySatisfiesRequiredDependency(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	handle := fwkplugin.NewEppHandle(context.Background(), nil)
	handle.AddPlugin(producer.TypedName().Name, producer)

	identityProvider, err := agentidentity.PluginFactory("custom-agent-identity", nil, handle)
	require.NoError(t, err)
	handle.AddPlugin(identityProvider.TypedName().Name, identityProvider)

	require.NoError(t, eppdatalayer.CreateMissingDataProducers(
		context.Background(),
		map[string]string{},
		map[string]fwkplugin.FactoryFunc{},
		handle,
	))

	ordered, err := eppdatalayer.ValidateAndOrderDataDependencies(handle.GetAllPlugins())
	require.NoError(t, err)
	assert.Equal(t, []string{identityProvider.TypedName().String(), producer.TypedName().String()}, ordered)
}

func TestLookupProducer(t *testing.T) {
	t.Parallel()

	handle := fwkplugin.NewEppHandle(context.Background(), nil)
	_, err := LookupProducer(handle)
	require.Error(t, err)
	_, err = LookupProducer(nil)
	require.Error(t, err)

	producer := newTestProducer(t)
	handle.AddPlugin(producer.TypedName().Name, producer)
	found, err := LookupProducer(handle)
	require.NoError(t, err)
	assert.Same(t, producer, found)
	assert.Equal(t, defaultEvictionTTL, found.EvictionTTL())
}

func TestFactoryParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		parameters      json.RawMessage
		wantTTL         time.Duration
		wantSweep       time.Duration
		wantErrContains string
	}{
		{
			name:       "custom durations",
			parameters: json.RawMessage(`{"evictionTtlSeconds":12.5,"evictionSweepSeconds":1.5}`),
			wantTTL:    12500 * time.Millisecond,
			wantSweep:  1500 * time.Millisecond,
		},
		{
			name:       "zero ttl disables eviction",
			parameters: json.RawMessage(`{"evictionTtlSeconds":0,"evictionSweepSeconds":10}`),
			wantTTL:    0,
			wantSweep:  10 * time.Second,
		},
		{
			name:            "negative ttl",
			parameters:      json.RawMessage(`{"evictionTtlSeconds":-1,"evictionSweepSeconds":10}`),
			wantErrContains: "evictionTtlSeconds must be >= 0",
		},
		{
			name:            "zero sweep",
			parameters:      json.RawMessage(`{"evictionSweepSeconds":0}`),
			wantErrContains: "evictionSweepSeconds must be > 0",
		},
		{
			name:            "negative sweep",
			parameters:      json.RawMessage(`{"evictionSweepSeconds":-1}`),
			wantErrContains: "evictionSweepSeconds must be > 0",
		},
		{
			name:            "unknown field",
			parameters:      json.RawMessage(`{"unknown":1}`),
			wantErrContains: "unknown field",
		},
		{
			name:            "invalid json",
			parameters:      json.RawMessage(`not-json`),
			wantErrContains: "invalid config",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plg, err := Factory("custom", fwkplugin.StrictDecoder(test.parameters), nil)
			if test.wantErrContains != "" {
				require.ErrorContains(t, err, test.wantErrContains)
				return
			}
			require.NoError(t, err)
			producer, ok := plg.(*Producer)
			require.True(t, ok)
			assert.Equal(t, test.wantTTL, producer.evictionTTL)
			assert.Equal(t, test.wantSweep, producer.evictionSweepInterval)
		})
	}
}

func TestRequestHeaderWithoutAgentIdentityIsNoOp(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	request := &fwksched.InferenceRequest{}

	require.NoError(t, producer.RequestHeader(context.Background(), nil))
	require.NoError(t, producer.RequestHeader(context.Background(), request))
	require.NoError(t, producer.RequestHeader(context.Background(), requestWithAgentIdentity("")))

	assert.Empty(t, request.AttributeKeys())
	assert.Zero(t, registrySessionCount(producer.Registry()))
}

func TestSessionIDAttributeAloneIsNoOp(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	request := &fwksched.InferenceRequest{}
	request.PutAttribute(attrsession.SessionIDDataKey, attrsession.SessionID("session-a"))

	require.NoError(t, producer.RequestHeader(context.Background(), request))
	require.NoError(t, producer.PreRequest(context.Background(), request, resultWithProfiles(1)))

	_, published := ReadSessionState(request)
	assert.False(t, published)
}

func TestRequestHeaderPublishesLiveState(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	queued := requestWithAgentIdentity("session-a")
	before := time.Now()
	require.NoError(t, producer.RequestHeader(context.Background(), queued))

	state, ok := ReadSessionState(queued)
	require.True(t, ok)
	assert.Equal(t, StatusNew, state.Status)
	assert.Zero(t, state.TurnsTaken)
	assert.False(t, state.LastSeenAt.Before(before))

	// Another turn of the same session dispatches while queued waits; the
	// attribute attached before queuing reflects it.
	startTurn(t, producer, resultFor(podA()))
	state, ok = ReadSessionState(queued)
	require.True(t, ok)
	assert.Equal(t, StatusRunning, state.Status)
	assert.Equal(t, int64(1), state.TurnsTaken)
	assert.Equal(t, int64(1), state.InFlightRequests)
	assert.Equal(t, "default/pod-a", state.BoundEndpoint)
	assert.GreaterOrEqual(t, state.Duration, time.Duration(0))
}

func TestPreRequestChargesPromptTokens(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	request := startTurn(t, producer, resultFor(podA()))
	state, ok := ReadSessionState(request)
	require.True(t, ok)
	assert.Equal(t, int64(100), state.InFlightTokens, "400 bytes estimate 100 tokens")

	tokenized := requestWithAgentIdentity("session-b")
	tokenized.Body = &fwkrh.InferenceRequestBody{TokenizedRequest: &fwkrh.TokenizedRequest{Prompts: []fwkrh.PromptTokens{{TokenIDs: make([]uint32, 7)}}}}
	require.NoError(t, producer.RequestHeader(context.Background(), tokenized))
	require.NoError(t, producer.PreRequest(context.Background(), tokenized, resultFor(podA())))
	state, ok = ReadSessionState(tokenized)
	require.True(t, ok)
	assert.Equal(t, int64(7), state.InFlightTokens, "the tokenized prompt length is preferred")
}

func TestResponseBodySettlesTurn(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	request := startTurn(t, producer, resultFor(podA()))

	producer.ResponseBody(context.Background(), request, &fwkrc.Response{EndOfStream: false}, nil)
	state := observed(t, producer)
	assert.Equal(t, int64(1), state.InFlightRequests)
	assert.Zero(t, state.CompletedRequests)

	response := finalResponse(fwkrc.TerminationCauseNatural, 12, 34)
	response.Usage.TotalTokens = 46
	response.FinishReason = "tool_calls"
	response.HasToolCalls = true
	producer.ResponseBody(context.Background(), request, response, nil)
	state = observed(t, producer)
	assert.Zero(t, state.InFlightRequests)
	assert.Zero(t, state.InFlightTokens)
	assert.Equal(t, int64(1), state.CompletedRequests)
	assert.Equal(t, int64(12), state.TotalInputTokens)
	assert.Equal(t, int64(34), state.TotalOutputTokens)
	assert.Equal(t, int64(46), state.CommittedTokens)
	assert.Equal(t, StatusActing, state.Status)
	assert.Equal(t, "tool_calls", state.LastFinishReason)
	assert.True(t, state.LastTurnHadToolCall)

	next := startTurn(t, producer, resultFor(podA()))
	stop := finalResponse(fwkrc.TerminationCauseNatural, 50, 10)
	stop.FinishReason = "stop"
	producer.ResponseBody(context.Background(), next, stop, nil)
	state = observed(t, producer)
	assert.Equal(t, StatusIdle, state.Status)
	assert.Equal(t, int64(60), state.CommittedTokens)
}

func TestAbnormalTerminationDoesNotCompleteRequest(t *testing.T) {
	t.Parallel()

	causes := []fwkrc.TerminationCause{
		fwkrc.TerminationCauseClientDisconnect,
		fwkrc.TerminationCauseEvicted,
		fwkrc.TerminationCauseError,
	}
	for _, cause := range causes {
		t.Run(string(cause), func(t *testing.T) {
			t.Parallel()
			producer := newTestProducer(t)
			request := startTurn(t, producer, resultWithProfiles(1))

			producer.ResponseBody(context.Background(), request, finalResponse(cause, 12, 34), nil)
			state := observed(t, producer)
			assert.Zero(t, state.InFlightRequests)
			assert.Zero(t, state.InFlightTokens)
			assert.Zero(t, state.CompletedRequests)
			assert.Zero(t, state.TotalInputTokens)
			assert.Zero(t, state.TotalOutputTokens)
		})
	}
}

func TestNaturalCompletionWithoutUsage(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	request := startTurn(t, producer, resultWithProfiles(1))
	producer.ResponseBody(context.Background(), request, finalResponse(fwkrc.TerminationCauseNatural, 0, 0), nil)

	state := observed(t, producer)
	assert.Equal(t, int64(1), state.CompletedRequests)
	assert.Zero(t, state.TotalInputTokens)
	assert.Zero(t, state.TotalOutputTokens)
}

func TestResponseBodyWithoutTrackedDispatchIsNoOp(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	request := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.RequestHeader(context.Background(), request))

	producer.ResponseBody(context.Background(), nil, finalResponse(fwkrc.TerminationCauseNatural, 1, 2), nil)
	producer.ResponseBody(context.Background(), &fwksched.InferenceRequest{}, finalResponse(fwkrc.TerminationCauseNatural, 1, 2), nil)
	producer.ResponseBody(context.Background(), request, nil, nil)
	producer.ResponseBody(context.Background(), request, finalResponse(fwkrc.TerminationCauseNatural, 1, 2), nil)

	state := observed(t, producer)
	assert.Zero(t, state.InFlightRequests)
	assert.Zero(t, state.CompletedRequests)
	assert.Zero(t, state.TotalInputTokens)
	assert.Zero(t, state.TotalOutputTokens)
	assert.Equal(t, StatusNew, state.Status)
}

func TestPreRequestCountsMultipleProfilesOnce(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	startTurn(t, producer, resultWithProfiles(2))

	state := observed(t, producer)
	assert.Equal(t, int64(1), state.TurnsTaken)
}

func TestExtractPublishesEndpointSummary(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	nn := types.NamespacedName{Namespace: "default", Name: "pod-a"}
	endpoint := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: nn}, &fwkdl.Metrics{})
	require.NoError(t, producer.Extract(context.Background(), fwkdl.EndpointEvent{Type: fwkdl.EventAddOrUpdate, Endpoint: endpoint}))

	summary, ok := ReadEndpointSessionSummary(endpoint.GetAttributes())
	require.True(t, ok)
	assert.Zero(t, summary.CapacityTokens, "capacity is unknown until metrics report it")

	// Metrics arrive after the endpoint is added; capacity is read on use.
	endpoint.UpdateMetrics(&fwkdl.Metrics{CacheBlockSize: 16, CacheNumBlocks: 100})
	request := startTurn(t, producer, resultFor(podA()))
	summary, ok = ReadEndpointSessionSummary(endpoint.GetAttributes())
	require.True(t, ok)
	assert.Equal(t, int64(1600), summary.CapacityTokens)
	assert.Equal(t, int64(100), summary.RunningTokens)
	assert.Equal(t, 1, summary.Sessions)

	stale := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: nn}, &fwkdl.Metrics{})
	require.NoError(t, producer.Extract(context.Background(), fwkdl.EndpointEvent{Type: fwkdl.EventDelete, Endpoint: stale}))
	state, ok := ReadSessionState(request)
	require.True(t, ok)
	assert.Equal(t, "default/pod-a", state.BoundEndpoint, "a delete for a replaced endpoint is ignored")

	require.NoError(t, producer.Extract(context.Background(), fwkdl.EndpointEvent{Type: fwkdl.EventDelete, Endpoint: endpoint}))
	state, ok = ReadSessionState(request)
	require.True(t, ok)
	assert.Empty(t, state.BoundEndpoint, "sessions on a deleted endpoint are unbound")
	_, ok = ReadEndpointSessionSummary(endpoint.GetAttributes())
	assert.False(t, ok)

	require.NoError(t, producer.Extract(context.Background(), fwkdl.EndpointEvent{Type: fwkdl.EventAddOrUpdate}))
}
