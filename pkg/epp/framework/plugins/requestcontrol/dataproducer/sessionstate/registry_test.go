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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func registrySessionCount(registry *SessionStateRegistry) int {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return len(registry.sessions)
}

func setRegistrySessionTimes(t *testing.T, registry *SessionStateRegistry, identity string, firstSeenAt, lastSeenAt time.Time) {
	t.Helper()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	record, ok := registry.sessions[identity]
	require.True(t, ok)
	record.firstSeenAt = firstSeenAt
	record.state.LastSeenAt = lastSeenAt
	record.publish()
}

func setLastResponseAt(t *testing.T, registry *SessionStateRegistry, identity string, at time.Time) {
	t.Helper()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	record, ok := registry.sessions[identity]
	require.True(t, ok)
	record.state.LastResponseAt = at
	record.publish()
}

func mustState(t *testing.T, registry *SessionStateRegistry, identity string) SessionState {
	t.Helper()
	state, ok := registry.State(identity)
	require.True(t, ok, "session %q must be tracked", identity)
	return state
}

func fixedCapacity(tokens int64) func() int64 {
	return func() int64 { return tokens }
}

func TestRegistryObserveCreatesNewSession(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	before := time.Now()
	registry.observe("session-a")
	after := time.Now()

	state := mustState(t, registry, "session-a")
	assert.Equal(t, StatusNew, state.Status)
	assert.Empty(t, state.BoundEndpoint)
	assert.Zero(t, state.TurnsTaken)
	assert.False(t, state.LastSeenAt.Before(before))
	assert.False(t, state.LastSeenAt.After(after))

	start := time.Now().Add(-2 * time.Minute)
	setRegistrySessionTimes(t, registry, "session-a", start, start)
	state = mustState(t, registry, "session-a")
	assert.GreaterOrEqual(t, state.Duration, 2*time.Minute)
	assert.Equal(t, start, state.LastSeenAt)

	registry.observe("session-a")
	assert.True(t, mustState(t, registry, "session-a").LastSeenAt.After(start))
}

func TestRegistryUnknownSession(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	_, ok := registry.State("missing")
	assert.False(t, ok)
	registry.RecordResponse("missing", TurnOutcome{NaturallyCompleted: true})
	assert.Zero(t, registrySessionCount(registry))
}

func TestRegistryLifecycleTransitions(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.observe("session-a")

	registry.RecordDispatch("session-a", "ns/pod-a", 100)
	state := mustState(t, registry, "session-a")
	assert.Equal(t, StatusRunning, state.Status)
	assert.Equal(t, "ns/pod-a", state.BoundEndpoint)
	assert.Equal(t, int64(1), state.TurnsTaken)
	assert.Equal(t, int64(1), state.InFlightRequests)
	assert.Equal(t, int64(100), state.InFlightTokens)
	assert.Equal(t, int64(100), state.WorkingSetTokens())

	registry.RecordResponse("session-a", TurnOutcome{
		NaturallyCompleted: true, InputTokens: 90, OutputTokens: 30, TotalTokens: 120,
		InFlightTokens: 100, FinishReason: "tool_calls", HasToolCalls: true,
	})
	state = mustState(t, registry, "session-a")
	assert.Equal(t, StatusActing, state.Status)
	assert.Equal(t, "ns/pod-a", state.BoundEndpoint)
	assert.Zero(t, state.InFlightRequests)
	assert.Zero(t, state.InFlightTokens)
	assert.Equal(t, int64(120), state.CommittedTokens)
	assert.Equal(t, int64(1), state.CompletedRequests)
	assert.Equal(t, int64(90), state.TotalInputTokens)
	assert.Equal(t, int64(30), state.TotalOutputTokens)
	assert.Equal(t, "tool_calls", state.LastFinishReason)
	assert.True(t, state.LastTurnHadToolCall)
	assert.False(t, state.LastResponseAt.IsZero())

	registry.RecordDispatch("session-a", "ns/pod-a", 150)
	state = mustState(t, registry, "session-a")
	assert.Equal(t, StatusRunning, state.Status)
	assert.Equal(t, int64(150), state.WorkingSetTokens(), "a running turn counts max(committed, in-flight)")

	registry.RecordResponse("session-a", TurnOutcome{
		NaturallyCompleted: true, InputTokens: 150, OutputTokens: 50, TotalTokens: 200,
		InFlightTokens: 150, FinishReason: "stop",
	})
	state = mustState(t, registry, "session-a")
	assert.Equal(t, StatusIdle, state.Status)
	assert.Equal(t, int64(200), state.CommittedTokens, "committed tokens are replaced, not summed")
	assert.Equal(t, int64(240), state.TotalInputTokens)
	assert.Equal(t, int64(80), state.TotalOutputTokens)
	assert.False(t, state.LastTurnHadToolCall)
}

func TestRegistryParallelTurnsStayRunning(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.RecordDispatch("session-a", "ns/pod-a", 10)
	registry.RecordDispatch("session-a", "ns/pod-a", 20)
	registry.RecordResponse("session-a", TurnOutcome{NaturallyCompleted: true, TotalTokens: 15, InFlightTokens: 10})

	state := mustState(t, registry, "session-a")
	assert.Equal(t, StatusRunning, state.Status)
	assert.Equal(t, int64(1), state.InFlightRequests)
	assert.Equal(t, int64(20), state.InFlightTokens)

	registry.RecordResponse("session-a", TurnOutcome{NaturallyCompleted: true, TotalTokens: 25, InFlightTokens: 20})
	assert.Equal(t, StatusIdle, mustState(t, registry, "session-a").Status)
}

func TestRegistryAbnormalResponse(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.RecordDispatch("session-a", "ns/pod-a", 40)
	registry.RecordResponse("session-a", TurnOutcome{NaturallyCompleted: false, InputTokens: 12, OutputTokens: 34, InFlightTokens: 40})
	state := mustState(t, registry, "session-a")
	assert.Zero(t, state.InFlightRequests)
	assert.Zero(t, state.CompletedRequests)
	assert.Zero(t, state.TotalInputTokens)
	assert.Zero(t, state.TotalOutputTokens)
	assert.Equal(t, int64(46), state.CommittedTokens, "partial usage still sizes the KV prefix")
	assert.Equal(t, StatusIdle, state.Status)

	registry.RecordResponse("session-a", TurnOutcome{NaturallyCompleted: true, InputTokens: 1, OutputTokens: 2})
	state = mustState(t, registry, "session-a")
	assert.Zero(t, state.CompletedRequests, "a response without a turn in flight is ignored")
}

func TestRegistryCommittedTokensFallsBackToCharge(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.RecordDispatch("session-a", "ns/pod-a", 40)
	registry.RecordResponse("session-a", TurnOutcome{NaturallyCompleted: true, InFlightTokens: 40})
	assert.Equal(t, int64(40), mustState(t, registry, "session-a").CommittedTokens)
}

func TestRegistryRebindMovesReverseIndex(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.UpsertEndpoint("ns/pod-a", nil)
	registry.UpsertEndpoint("ns/pod-b", nil)
	registry.RecordDispatch("session-a", "ns/pod-a", 10)
	registry.RecordResponse("session-a", TurnOutcome{NaturallyCompleted: true, TotalTokens: 30, InFlightTokens: 10})
	registry.RecordDispatch("session-a", "ns/pod-b", 40)
	state := mustState(t, registry, "session-a")
	assert.Equal(t, "ns/pod-b", state.BoundEndpoint)
	assert.Equal(t, "ns/pod-b", state.LastBoundEndpoint)

	a, ok := registry.EndpointSummary("ns/pod-a")
	require.True(t, ok)
	assert.Zero(t, a.Sessions)
	b, ok := registry.EndpointSummary("ns/pod-b")
	require.True(t, ok)
	assert.Equal(t, 1, b.Sessions)
	assert.Equal(t, int64(40), b.RunningTokens)
}

func TestRegistryEndpointSummaryByStatus(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.UpsertEndpoint("ns/pod-a", fixedCapacity(10000))
	// running: max(committed 0, in-flight 100)
	registry.RecordDispatch("running", "ns/pod-a", 100)
	// acting: committed 200
	registry.RecordDispatch("acting", "ns/pod-a", 1)
	registry.RecordResponse("acting", TurnOutcome{NaturallyCompleted: true, TotalTokens: 200, InFlightTokens: 1, HasToolCalls: true})
	// idle: committed 300
	registry.RecordDispatch("idle", "ns/pod-a", 1)
	registry.RecordResponse("idle", TurnOutcome{NaturallyCompleted: true, TotalTokens: 300, InFlightTokens: 1})
	// archived: committed 400
	registry.RecordDispatch("archived", "ns/pod-a", 1)
	registry.RecordResponse("archived", TurnOutcome{NaturallyCompleted: true, TotalTokens: 400, InFlightTokens: 1})
	require.True(t, registry.Archive("archived"))

	summary, ok := registry.EndpointSummary("ns/pod-a")
	require.True(t, ok)
	assert.Equal(t, EndpointSessionSummary{
		CapacityTokens: 10000,
		RunningTokens:  100,
		ActingTokens:   200,
		IdleTokens:     300,
		ArchivedTokens: 400,
		Sessions:       4,
	}, summary)
	assert.Equal(t, int64(1000), summary.TotalTokens())

	seen := map[string]LifecycleStatus{}
	registry.ForEachEndpointSession("ns/pod-a", func(identity string, state SessionState) bool {
		seen[identity] = state.Status
		return true
	})
	assert.Equal(t, map[string]LifecycleStatus{
		"running": StatusRunning, "acting": StatusActing, "idle": StatusIdle, "archived": StatusArchived,
	}, seen)

	endpoints := 0
	registry.ForEachEndpoint(func(id string, s EndpointSessionSummary) bool {
		endpoints++
		assert.Equal(t, "ns/pod-a", id)
		assert.Equal(t, summary, s)
		return true
	})
	assert.Equal(t, 1, endpoints)

	_, ok = registry.EndpointSummary("ns/unknown")
	assert.False(t, ok)
}

func TestRegistryArchive(t *testing.T) {
	t.Parallel()

	t.Run("unknown session", func(t *testing.T) {
		t.Parallel()
		registry := &SessionStateRegistry{}
		assert.False(t, registry.Archive("missing"))
	})

	t.Run("unbound session is removed", func(t *testing.T) {
		t.Parallel()
		registry := &SessionStateRegistry{}
		registry.observe("session-a")
		assert.True(t, registry.Archive("session-a"))
		assert.Zero(t, registrySessionCount(registry))
	})

	t.Run("in-flight session is archived when its last turn ends", func(t *testing.T) {
		t.Parallel()
		registry := &SessionStateRegistry{}
		registry.RecordDispatch("session-a", "ns/pod-a", 10)
		assert.True(t, registry.Archive("session-a"))
		assert.Equal(t, StatusRunning, mustState(t, registry, "session-a").Status)

		registry.RecordResponse("session-a", TurnOutcome{NaturallyCompleted: true, TotalTokens: 30, InFlightTokens: 10, HasToolCalls: true})
		assert.Equal(t, StatusArchived, mustState(t, registry, "session-a").Status)
	})

	t.Run("a new turn resumes an archived session", func(t *testing.T) {
		t.Parallel()
		registry := &SessionStateRegistry{}
		registry.RecordDispatch("session-a", "ns/pod-a", 10)
		registry.RecordResponse("session-a", TurnOutcome{NaturallyCompleted: true, TotalTokens: 30, InFlightTokens: 10})
		require.True(t, registry.Archive("session-a"))
		registry.RecordDispatch("session-a", "ns/pod-a", 40)
		state := mustState(t, registry, "session-a")
		assert.Equal(t, StatusRunning, state.Status)
		assert.Equal(t, "ns/pod-a", state.BoundEndpoint)
	})
}

func TestRegistryRemoveEndpoint(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.UpsertEndpoint("ns/pod-a", nil)
	registry.RecordDispatch("idle", "ns/pod-a", 1)
	registry.RecordResponse("idle", TurnOutcome{NaturallyCompleted: true, TotalTokens: 300, InFlightTokens: 1})
	registry.RecordDispatch("archived", "ns/pod-a", 1)
	registry.RecordResponse("archived", TurnOutcome{NaturallyCompleted: true, TotalTokens: 400, InFlightTokens: 1})
	require.True(t, registry.Archive("archived"))

	registry.RemoveEndpoint("ns/pod-a")
	registry.RemoveEndpoint("ns/unknown")

	state := mustState(t, registry, "idle")
	assert.Empty(t, state.BoundEndpoint)
	assert.Equal(t, "ns/pod-a", state.LastBoundEndpoint)
	assert.Equal(t, int64(300), state.CommittedTokens)
	assert.False(t, registry.HasEndpoint("ns/pod-a"))
	_, ok := registry.State("archived")
	assert.False(t, ok)
	_, ok = registry.EndpointSummary("ns/pod-a")
	assert.False(t, ok)
}

func TestRegistryCapacityEvictionOrder(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.UpsertEndpoint("ns/pod-a", fixedCapacity(1000))
	finish := func(id string, total int64, toolCall bool) {
		registry.RecordDispatch(id, "ns/pod-a", 1)
		registry.RecordResponse(id, TurnOutcome{NaturallyCompleted: true, TotalTokens: total, InFlightTokens: 1, HasToolCalls: toolCall})
	}
	finish("acting-old", 200, true)
	finish("idle-old", 200, false)
	finish("idle-new", 200, false)
	finish("archived", 200, false)
	require.True(t, registry.Archive("archived"))
	base := time.Now().Add(-time.Hour)
	setLastResponseAt(t, registry, "acting-old", base)
	setLastResponseAt(t, registry, "idle-old", base.Add(time.Minute))
	setLastResponseAt(t, registry, "idle-new", base.Add(2*time.Minute))

	// 800 committed + 450 running = 1250 > 1000: evict 250.
	registry.RecordDispatch("runner", "ns/pod-a", 450)

	_, ok := registry.State("archived")
	assert.False(t, ok, "archived sessions are evicted first and removed once empty")
	idleOld := mustState(t, registry, "idle-old")
	assert.Equal(t, int64(150), idleOld.CommittedTokens, "the oldest idle session is evicted next")
	assert.Equal(t, "ns/pod-a", idleOld.BoundEndpoint)
	assert.Equal(t, int64(200), mustState(t, registry, "idle-new").CommittedTokens)
	assert.Equal(t, int64(200), mustState(t, registry, "acting-old").CommittedTokens)

	// Grow the runner by 400 more: evicts idle-old (150), idle-new (200), then 50 from acting.
	registry.RecordDispatch("runner", "ns/pod-a", 400)
	idleOld = mustState(t, registry, "idle-old")
	assert.Zero(t, idleOld.CommittedTokens)
	assert.Empty(t, idleOld.BoundEndpoint, "a fully evicted session is unbound")
	assert.Equal(t, "ns/pod-a", idleOld.LastBoundEndpoint)
	assert.Empty(t, mustState(t, registry, "idle-new").BoundEndpoint)
	assert.Equal(t, int64(150), mustState(t, registry, "acting-old").CommittedTokens)

	summary, ok := registry.EndpointSummary("ns/pod-a")
	require.True(t, ok)
	assert.Equal(t, int64(1000), summary.TotalTokens())
}

func TestRegistryUnregisteredEndpointIsHidden(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.UpsertEndpoint("ns/pod-a", fixedCapacity(500))
	// A turn dispatched to an endpoint whose delete event already arrived.
	registry.RecordDispatch("session-a", "ns/gone", 10)
	assert.Equal(t, "ns/gone", mustState(t, registry, "session-a").BoundEndpoint)

	assert.True(t, registry.HasEndpoint("ns/pod-a"))
	assert.False(t, registry.HasEndpoint("ns/gone"))
	assert.Equal(t, map[string]int64{"ns/pod-a": 500}, registry.EndpointCapacities())
	_, ok := registry.EndpointSummary("ns/gone")
	assert.False(t, ok)
	registry.ForEachEndpoint(func(id string, _ EndpointSessionSummary) bool {
		assert.Equal(t, "ns/pod-a", id)
		return true
	})

	registry.RecordDispatch("session-a", "ns/pod-a", 10)
	registry.mu.Lock()
	_, kept := registry.endpoints["ns/gone"]
	registry.mu.Unlock()
	assert.False(t, kept, "an unregistered endpoint is dropped once no session is bound to it")
}

func TestRegistryUnknownCapacitySkipsEviction(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.UpsertEndpoint("ns/pod-a", fixedCapacity(0))
	registry.RecordDispatch("idle", "ns/pod-a", 1)
	registry.RecordResponse("idle", TurnOutcome{NaturallyCompleted: true, TotalTokens: 5000, InFlightTokens: 1})
	registry.RecordDispatch("runner", "ns/pod-a", 5000)
	assert.Equal(t, int64(5000), mustState(t, registry, "idle").CommittedTokens)
}

func TestRegistryEvictsIdleSessions(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.UpsertEndpoint("ns/pod-a", nil)
	registry.RecordDispatch("session-a", "ns/pod-a", 1)
	ttl := time.Hour
	start := time.Now().Add(-2 * ttl)
	setRegistrySessionTimes(t, registry, "session-a", start, start)

	registry.EvictIdle(start.Add(ttl+time.Nanosecond), ttl)
	assert.Equal(t, 1, registrySessionCount(registry), "a session with an in-flight request must remain")

	registry.RecordResponse("session-a", TurnOutcome{InFlightTokens: 1})
	setRegistrySessionTimes(t, registry, "session-a", start, start)
	registry.EvictIdle(start.Add(ttl), ttl)
	assert.Equal(t, 1, registrySessionCount(registry), "a session at the TTL boundary must remain")

	registry.EvictIdle(start.Add(ttl+time.Nanosecond), ttl)
	assert.Zero(t, registrySessionCount(registry))
	summary, ok := registry.EndpointSummary("ns/pod-a")
	require.True(t, ok)
	assert.Zero(t, summary.Sessions, "eviction removes the session from the reverse index")
}

func TestRegistryZeroTTLDisablesEviction(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.observe("session-a")
	registry.EvictIdle(time.Now().Add(24*time.Hour), 0)
	assert.Equal(t, 1, registrySessionCount(registry))
}

func TestRegistryResolveAfterEviction(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	record := registry.observe("session-a")
	registry.EvictIdle(time.Now().Add(2*time.Hour), time.Hour)

	_, ok := registry.resolve("session-a", record)
	assert.False(t, ok, "an evicted session reads as absent")

	registry.RecordDispatch("session-a", "ns/pod-a", 1)
	state, ok := registry.resolve("session-a", record)
	require.True(t, ok, "a recreated session is found by identity")
	assert.Equal(t, int64(1), state.TurnsTaken)
}

func TestRegistryConcurrentUpdates(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	const requests = 100

	var wg sync.WaitGroup
	for range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			record := registry.observe("session-a")
			registry.RecordDispatch("session-a", "ns/pod-a", 5)
			_, _ = registry.resolve("session-a", record)
			registry.RecordResponse("session-a", TurnOutcome{NaturallyCompleted: true, InputTokens: 2, OutputTokens: 3, InFlightTokens: 5})
		}()
	}
	wg.Wait()

	state := mustState(t, registry, "session-a")
	assert.Equal(t, int64(requests), state.TurnsTaken)
	assert.Zero(t, state.InFlightRequests)
	assert.Zero(t, state.InFlightTokens)
	assert.Equal(t, int64(requests), state.CompletedRequests)
	assert.Equal(t, int64(2*requests), state.TotalInputTokens)
	assert.Equal(t, int64(3*requests), state.TotalOutputTokens)
	assert.Equal(t, StatusIdle, state.Status)
}
