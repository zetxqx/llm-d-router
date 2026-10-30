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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"
)

type sessionRecord struct {
	firstSeenAt time.Time

	// state and archiveOnIdle are guarded by SessionStateRegistry.mu.
	state SessionState
	// archiveOnIdle defers an archive request received while a turn is in
	// flight until the last turn ends.
	archiveOnIdle bool

	// published is a copy of state stored after every mutation, so request
	// attribute reads do not take the registry lock.
	published atomic.Pointer[SessionState]
	// evicted is set when the record leaves the registry, so a reader holding
	// the record falls back to a lookup by identity.
	evicted atomic.Bool
}

func (rec *sessionRecord) publish() {
	state := rec.state
	rec.published.Store(&state)
}

func (rec *sessionRecord) load(now time.Time) SessionState {
	state := *rec.published.Load()
	state.Duration = now.Sub(rec.firstSeenAt)
	return state
}

type endpointRecord struct {
	// registered is set by UpsertEndpoint. A record created only because a
	// turn was dispatched to an endpoint the registry was never told about
	// is not reported and is dropped once no session is bound to it.
	registered bool
	// capacity returns the endpoint's KV capacity in tokens, or 0 when
	// unknown. It is resolved on use because endpoint metrics arrive after the
	// endpoint is added.
	capacity func() int64
	sessions map[string]*sessionRecord
}

func (e *endpointRecord) capacityTokens() int64 {
	if e.capacity == nil {
		return 0
	}
	return max(e.capacity(), 0)
}

// TurnOutcome describes how a dispatched turn ended.
type TurnOutcome struct {
	// NaturallyCompleted reports whether the response ended naturally. Only
	// such turns contribute to CompletedRequests and the token totals.
	NaturallyCompleted bool
	InputTokens        int64
	OutputTokens       int64
	TotalTokens        int64
	// InFlightTokens is the amount RecordDispatch charged for this turn.
	InFlightTokens int64
	FinishReason   string
	HasToolCalls   bool
}

// SessionStateRegistry stores per-session state and the reverse index of
// sessions bound to each endpoint, and owns their concurrency rules. Its zero
// value is ready for use and reads the real clock.
type SessionStateRegistry struct {
	clock     clock.PassiveClock
	mu        sync.Mutex
	sessions  map[string]*sessionRecord
	endpoints map[string]*endpointRecord
}

// NewSessionStateRegistry returns an empty registry that reads time from clk,
// or from the real clock when clk is nil.
func NewSessionStateRegistry(clk clock.PassiveClock) *SessionStateRegistry {
	return &SessionStateRegistry{clock: clk}
}

func (r *SessionStateRegistry) now() time.Time {
	if r.clock == nil {
		return time.Now()
	}
	return r.clock.Now()
}

// State returns the current state of a session, or false if the registry does
// not track it.
func (r *SessionStateRegistry) State(identity string) (SessionState, bool) {
	r.mu.Lock()
	record, ok := r.sessions[identity]
	r.mu.Unlock()
	if !ok {
		return SessionState{}, false
	}
	return record.load(r.now()), true
}

// observe marks the session as seen, creating it in StatusNew on first sight,
// and returns its record.
func (r *SessionStateRegistry) observe(identity string) *sessionRecord {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()

	record := r.getOrCreateLocked(identity, now)
	record.state.LastSeenAt = now
	record.publish()
	return record
}

// resolve returns the current state for a record obtained from observe. A
// record that has since been evicted is looked up again by identity.
func (r *SessionStateRegistry) resolve(identity string, record *sessionRecord) (SessionState, bool) {
	if record != nil && !record.evicted.Load() {
		return record.load(r.now()), true
	}
	return r.State(identity)
}

// RecordDispatch records one turn dispatched to endpointID, binds the session
// to it, and charges inFlightTokens until the turn ends. An empty endpointID
// records the turn without binding.
func (r *SessionStateRegistry) RecordDispatch(identity, endpointID string, inFlightTokens int64) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()

	record := r.getOrCreateLocked(identity, now)
	state := &record.state
	state.TurnsTaken++
	state.InFlightRequests++
	state.InFlightTokens += max(inFlightTokens, 0)
	state.LastSeenAt = now
	record.archiveOnIdle = false
	if endpointID != "" {
		r.bindLocked(identity, record, endpointID, now)
	}
	setStatusLocked(record, StatusRunning, now)
	record.publish()
	r.enforceCapacityLocked(state.BoundEndpoint, now)
}

// RecordResponse records the end of a dispatched turn's response lifecycle.
// Responses for sessions without a turn in flight are ignored.
func (r *SessionStateRegistry) RecordResponse(identity string, outcome TurnOutcome) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()

	record, exists := r.sessions[identity]
	if !exists || record.state.InFlightRequests == 0 {
		return
	}
	state := &record.state
	state.InFlightRequests--
	state.InFlightTokens = max(state.InFlightTokens-max(outcome.InFlightTokens, 0), 0)
	state.LastSeenAt = now
	state.LastResponseAt = now
	state.LastFinishReason = outcome.FinishReason
	state.LastTurnHadToolCall = outcome.HasToolCalls
	switch {
	case outcome.TotalTokens > 0:
		state.CommittedTokens = outcome.TotalTokens
	case outcome.InputTokens+outcome.OutputTokens > 0:
		state.CommittedTokens = outcome.InputTokens + outcome.OutputTokens
	case outcome.InFlightTokens > state.CommittedTokens:
		// Without usage, the prompt the turn sent is the best size estimate.
		state.CommittedTokens = outcome.InFlightTokens
	}
	if outcome.NaturallyCompleted {
		state.CompletedRequests++
		state.TotalInputTokens += outcome.InputTokens
		state.TotalOutputTokens += outcome.OutputTokens
	}
	if state.InFlightRequests == 0 {
		next := StatusIdle
		switch {
		case record.archiveOnIdle:
			next = StatusArchived
			record.archiveOnIdle = false
		case outcome.HasToolCalls:
			next = StatusActing
		}
		setStatusLocked(record, next, now)
	}
	record.publish()
	r.enforceCapacityLocked(state.BoundEndpoint, now)
}

// Archive marks a session as archived, so its KV blocks may be released. A
// session with a turn in flight is archived when its last turn ends. An
// archived session that holds no KV blocks is removed. Archive reports
// whether the registry tracks the session.
func (r *SessionStateRegistry) Archive(identity string) bool {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()

	record, exists := r.sessions[identity]
	if !exists {
		return false
	}
	if record.state.InFlightRequests > 0 {
		record.archiveOnIdle = true
		return true
	}
	setStatusLocked(record, StatusArchived, now)
	if record.state.BoundEndpoint == "" || record.state.CommittedTokens == 0 {
		r.deleteLocked(identity, record)
		return true
	}
	record.publish()
	return true
}

// EvictIdle removes sessions that have been idle longer than ttl. Sessions
// with in-flight requests are retained until those requests end.
func (r *SessionStateRegistry) EvictIdle(now time.Time, ttl time.Duration) {
	if ttl <= 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for identity, record := range r.sessions {
		if record.state.InFlightRequests > 0 {
			continue
		}
		if now.Sub(record.state.LastSeenAt) > ttl {
			r.deleteLocked(identity, record)
		}
	}
}

// UpsertEndpoint registers an endpoint and how to read its KV capacity in
// tokens. capacity may be nil when the capacity is unknown.
func (r *SessionStateRegistry) UpsertEndpoint(endpointID string, capacity func() int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	endpoint := r.ensureEndpointLocked(endpointID)
	endpoint.registered = true
	endpoint.capacity = capacity
}

// HasEndpoint reports whether endpointID is registered.
func (r *SessionStateRegistry) HasEndpoint(endpointID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	endpoint, ok := r.endpoints[endpointID]
	return ok && endpoint.registered
}

// EndpointCapacities returns the KV capacity in tokens of every registered
// endpoint, 0 when unknown, without aggregating their sessions.
func (r *SessionStateRegistry) EndpointCapacities() map[string]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	capacities := make(map[string]int64, len(r.endpoints))
	for id, endpoint := range r.endpoints {
		if endpoint.registered {
			capacities[id] = endpoint.capacityTokens()
		}
	}
	return capacities
}

// RemoveEndpoint drops an endpoint that left the pool. Its sessions are
// unbound and keep CommittedTokens, since their next turn resends the same
// history; archived sessions are removed.
func (r *SessionStateRegistry) RemoveEndpoint(endpointID string) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()

	endpoint, ok := r.endpoints[endpointID]
	if !ok {
		return
	}
	for identity, record := range endpoint.sessions {
		if record.state.Status == StatusArchived {
			r.deleteLocked(identity, record)
			continue
		}
		r.unbindLocked(identity, record, now)
		record.publish()
	}
	delete(r.endpoints, endpointID)
}

// EndpointSummary returns the aggregated working set of the sessions bound to
// endpointID, or false if the endpoint is not registered.
func (r *SessionStateRegistry) EndpointSummary(endpointID string) (EndpointSessionSummary, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	endpoint, ok := r.endpoints[endpointID]
	if !ok || !endpoint.registered {
		return EndpointSessionSummary{}, false
	}
	return summarizeLocked(endpoint), true
}

// ForEachEndpoint calls fn for every registered endpoint until fn returns
// false. fn runs under the registry lock and must not call the registry.
func (r *SessionStateRegistry) ForEachEndpoint(fn func(endpointID string, summary EndpointSessionSummary) bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, endpoint := range r.endpoints {
		if !endpoint.registered {
			continue
		}
		if !fn(id, summarizeLocked(endpoint)) {
			return
		}
	}
}

// ForEachEndpointSession calls fn for every session bound to endpointID until
// fn returns false. fn runs under the registry lock and must not call the
// registry.
func (r *SessionStateRegistry) ForEachEndpointSession(endpointID string, fn func(identity string, state SessionState) bool) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	endpoint, ok := r.endpoints[endpointID]
	if !ok {
		return
	}
	for identity, record := range endpoint.sessions {
		state := record.state
		state.Duration = now.Sub(record.firstSeenAt)
		if !fn(identity, state) {
			return
		}
	}
}

func summarizeLocked(endpoint *endpointRecord) EndpointSessionSummary {
	summary := EndpointSessionSummary{
		CapacityTokens: endpoint.capacityTokens(),
		Sessions:       len(endpoint.sessions),
	}
	for _, record := range endpoint.sessions {
		state := &record.state
		switch state.Status {
		case StatusRunning:
			summary.RunningTokens += state.WorkingSetTokens()
		case StatusActing:
			summary.ActingTokens += state.CommittedTokens
		case StatusIdle:
			summary.IdleTokens += state.CommittedTokens
		case StatusArchived:
			summary.ArchivedTokens += state.CommittedTokens
		}
	}
	return summary
}

// enforceCapacityLocked models an endpoint evicting KV blocks when the
// sessions bound to it exceed its capacity. Blocks are taken from sessions
// with no turn in flight in eviction order (see evictsBefore). A session whose
// CommittedTokens reaches 0 is unbound, and removed if archived. Endpoints with
// unknown capacity are skipped.
func (r *SessionStateRegistry) enforceCapacityLocked(endpointID string, now time.Time) {
	if endpointID == "" {
		return
	}
	endpoint, ok := r.endpoints[endpointID]
	if !ok {
		return
	}
	capacity := endpoint.capacityTokens()
	if capacity <= 0 {
		return
	}
	excess := summarizeLocked(endpoint).TotalTokens() - capacity
	if excess <= 0 {
		return
	}

	type victim struct {
		identity string
		record   *sessionRecord
	}
	victims := make([]victim, 0, len(endpoint.sessions))
	for identity, record := range endpoint.sessions {
		if record.state.Status != StatusRunning && record.state.CommittedTokens > 0 {
			victims = append(victims, victim{identity: identity, record: record})
		}
	}
	sort.Slice(victims, func(i, j int) bool {
		return evictsBefore(&victims[i].record.state, &victims[j].record.state)
	})
	for _, v := range victims {
		if excess <= 0 {
			break
		}
		state := &v.record.state
		evicted := min(state.CommittedTokens, excess)
		state.CommittedTokens -= evicted
		excess -= evicted
		if state.CommittedTokens > 0 {
			v.record.publish()
			continue
		}
		if state.Status == StatusArchived {
			r.deleteLocked(v.identity, v.record)
			continue
		}
		r.unbindLocked(v.identity, v.record, now)
		v.record.publish()
	}
}

// evictsBefore orders eviction candidates: archived sessions first, then idle,
// then acting, each oldest first.
func evictsBefore(a, b *SessionState) bool {
	if ra, rb := evictionRank(a.Status), evictionRank(b.Status); ra != rb {
		return ra < rb
	}
	return lastActivity(a).Before(lastActivity(b))
}

func evictionRank(status LifecycleStatus) int {
	switch status {
	case StatusArchived:
		return 0
	case StatusIdle:
		return 1
	default:
		return 2
	}
}

func lastActivity(s *SessionState) time.Time {
	if s.Status == StatusArchived {
		return s.LastStateChangeAt
	}
	return s.LastResponseAt
}

func setStatusLocked(record *sessionRecord, status LifecycleStatus, now time.Time) {
	if record.state.Status == status {
		return
	}
	record.state.Status = status
	record.state.LastStateChangeAt = now
}

func (r *SessionStateRegistry) bindLocked(identity string, record *sessionRecord, endpointID string, now time.Time) {
	if record.state.BoundEndpoint == endpointID {
		r.ensureEndpointLocked(endpointID).sessions[identity] = record
		return
	}
	r.unbindLocked(identity, record, now)
	r.ensureEndpointLocked(endpointID).sessions[identity] = record
	record.state.BoundEndpoint = endpointID
	record.state.LastBoundEndpoint = endpointID
	record.state.LastStateChangeAt = now
}

func (r *SessionStateRegistry) unbindLocked(identity string, record *sessionRecord, now time.Time) {
	if record.state.BoundEndpoint == "" {
		return
	}
	r.removeFromEndpointLocked(identity, record.state.BoundEndpoint)
	record.state.BoundEndpoint = ""
	record.state.LastStateChangeAt = now
}

func (r *SessionStateRegistry) deleteLocked(identity string, record *sessionRecord) {
	r.removeFromEndpointLocked(identity, record.state.BoundEndpoint)
	delete(r.sessions, identity)
	record.evicted.Store(true)
}

func (r *SessionStateRegistry) removeFromEndpointLocked(identity, endpointID string) {
	endpoint, ok := r.endpoints[endpointID]
	if !ok {
		return
	}
	delete(endpoint.sessions, identity)
	if !endpoint.registered && len(endpoint.sessions) == 0 {
		delete(r.endpoints, endpointID)
	}
}

func (r *SessionStateRegistry) ensureEndpointLocked(endpointID string) *endpointRecord {
	if r.endpoints == nil {
		r.endpoints = make(map[string]*endpointRecord)
	}
	endpoint, ok := r.endpoints[endpointID]
	if !ok {
		endpoint = &endpointRecord{sessions: make(map[string]*sessionRecord)}
		r.endpoints[endpointID] = endpoint
	}
	return endpoint
}

func (r *SessionStateRegistry) getOrCreateLocked(identity string, now time.Time) *sessionRecord {
	if r.sessions == nil {
		r.sessions = make(map[string]*sessionRecord)
	}
	if record, exists := r.sessions[identity]; exists {
		return record
	}
	record := &sessionRecord{
		firstSeenAt: now,
		state: SessionState{
			Status:            StatusNew,
			LastSeenAt:        now,
			LastStateChangeAt: now,
		},
	}
	record.publish()
	r.sessions[identity] = record
	return record
}
