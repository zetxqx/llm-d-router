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
	"time"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// SessionStateProducerType is the plugin type registered with the framework.
const SessionStateProducerType = "session-state-producer"

// SessionStateDataKey identifies session state published on the request
// attribute store. The default producer is the session-state-producer.
var SessionStateDataKey = plugin.NewDataKey("SessionStateDataKey", SessionStateProducerType)

// EndpointSessionDataKey identifies the EndpointSessionSummary published on
// endpoint attributes by the session-state-producer.
var EndpointSessionDataKey = plugin.NewDataKey("EndpointSessionDataKey", SessionStateProducerType)

// LifecycleStatus is the lifecycle stage of a session, derived only from the
// request and response lifecycle or from an explicit archive signal.
type LifecycleStatus string

const (
	// StatusNew is a session whose first turn has arrived but has not been
	// dispatched to any endpoint.
	StatusNew LifecycleStatus = "new"
	// StatusRunning is a session with at least one turn executing on
	// BoundEndpoint.
	StatusRunning LifecycleStatus = "running"
	// StatusActing is a session with no turn in flight whose latest turn ended
	// in a tool call, so a follow-up turn carrying the tool result is expected.
	StatusActing LifecycleStatus = "acting"
	// StatusIdle is a session with no turn in flight whose latest turn ended
	// without a tool call, so it waits for new user input.
	StatusIdle LifecycleStatus = "idle"
	// StatusArchived is a session the client or harness archived. Its KV
	// blocks on BoundEndpoint may be released.
	StatusArchived LifecycleStatus = "archived"
)

// SessionState is the observed state of one session.
type SessionState struct {
	// TurnsTaken is the number of turns dispatched for the session.
	TurnsTaken int64
	// Duration is the time elapsed since the session was first observed,
	// computed when the state is read.
	Duration time.Duration
	// LastSeenAt is the time of the latest request, dispatch, or response
	// activity. The idle eviction sweep compares it with its TTL.
	LastSeenAt time.Time
	// InFlightRequests is the number of dispatched turns whose response
	// lifecycle has not ended.
	InFlightRequests int64
	// CompletedRequests is the number of turns whose responses ended
	// naturally.
	CompletedRequests int64
	// TotalInputTokens is the sum of prompt tokens over naturally completed
	// turns.
	TotalInputTokens int64
	// TotalOutputTokens is the sum of completion tokens over naturally
	// completed turns.
	TotalOutputTokens int64

	// Status is the session's lifecycle stage.
	Status LifecycleStatus
	// BoundEndpoint is the NamespacedName string of the endpoint holding the
	// session's KV cache, or empty when the session is not bound.
	BoundEndpoint string
	// LastBoundEndpoint is the endpoint the session was most recently bound
	// to. It equals BoundEndpoint while bound and keeps its value after the
	// session is unbound, by capacity eviction or by the endpoint leaving the
	// pool.
	LastBoundEndpoint string
	// CommittedTokens is the total token count (prompt plus completion) of the
	// latest finished turn. Each turn resends the conversation history, so it
	// replaces the previous value and approximates the size of the session's
	// KV prefix on BoundEndpoint.
	CommittedTokens int64
	// InFlightTokens is the sum of the full prompt lengths of the session's
	// turns in flight.
	InFlightTokens int64
	// LastStateChangeAt is the time Status or BoundEndpoint last changed.
	LastStateChangeAt time.Time
	// LastResponseAt is the time the latest turn's response lifecycle ended.
	LastResponseAt time.Time
	// LastFinishReason is the finish reason reported for the latest finished
	// turn, such as "stop", "tool_calls" or "length".
	LastFinishReason string
	// LastTurnHadToolCall reports whether the latest finished turn invoked a
	// tool call.
	LastTurnHadToolCall bool
}

// WorkingSetTokens is the session's KV footprint on BoundEndpoint. A running
// turn resends the history covered by CommittedTokens, so the larger of the
// two is taken rather than their sum.
func (s SessionState) WorkingSetTokens() int64 {
	if s.Status == StatusRunning {
		return max(s.CommittedTokens, s.InFlightTokens)
	}
	return s.CommittedTokens
}

// EndpointSessionSummary aggregates the KV working set of the sessions bound to
// one endpoint, by session status.
type EndpointSessionSummary struct {
	// CapacityTokens is the endpoint's KV cache capacity in tokens
	// (CacheBlockSize * CacheNumBlocks), or 0 when the endpoint does not
	// report it.
	CapacityTokens int64
	// RunningTokens is the sum of WorkingSetTokens over StatusRunning sessions.
	RunningTokens int64
	// ActingTokens is the sum of CommittedTokens over StatusActing sessions.
	ActingTokens int64
	// IdleTokens is the sum of CommittedTokens over StatusIdle sessions.
	IdleTokens int64
	// ArchivedTokens is the sum of CommittedTokens over StatusArchived
	// sessions. These blocks can be released first.
	ArchivedTokens int64
	// Sessions is the number of sessions bound to the endpoint.
	Sessions int
}

// TotalTokens is the endpoint's session working set across all statuses.
func (s EndpointSessionSummary) TotalTokens() int64 {
	return s.RunningTokens + s.ActingTokens + s.IdleTokens + s.ArchivedTokens
}

// Clone implements datalayer.Cloneable.
func (s *EndpointSessionSummary) Clone() fwkdl.Cloneable {
	if s == nil {
		return nil
	}
	clone := *s
	return &clone
}

// ReadSessionState returns the SessionState published by the default producer
// on the request attribute store, or the zero value and false if absent. The
// producer publishes a dynamic attribute, so every call returns the current
// state.
func ReadSessionState(r *fwksched.InferenceRequest) (SessionState, bool) {
	key := SessionStateDataKey.WithNonEmptyProducerName(SessionStateProducerType)
	return fwksched.ReadRequestAttribute[SessionState](r, key)
}

// ReadEndpointSessionSummary returns the EndpointSessionSummary published by the
// default producer on the endpoint, or the zero value and false if absent.
func ReadEndpointSessionSummary(endpoint fwkdl.AttributeMap) (EndpointSessionSummary, bool) {
	if endpoint == nil {
		return EndpointSessionSummary{}, false
	}
	key := EndpointSessionDataKey.WithNonEmptyProducerName(SessionStateProducerType)
	summary, ok := fwkdl.ReadAttribute[*EndpointSessionSummary](endpoint, key)
	if !ok || summary == nil {
		return EndpointSessionSummary{}, false
	}
	return *summary, true
}
