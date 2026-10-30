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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionstate"
)

func TestEstimateTokens(t *testing.T) {
	require.Equal(t, int64(0), estimateTokens(0))
	require.Equal(t, int64(0), estimateTokens(-1))
	require.Equal(t, int64(100), estimateTokens(400))
}

// Real scraped capacity wins over the configured fallback.
func TestPodCapacity(t *testing.T) {
	a := newTestAgent(testConfig())
	require.Equal(t, float64(16000), a.podCapacity(sessionstate.EndpointSessionSummary{CapacityTokens: 16000}))
	require.Equal(t, float64(1000), a.podCapacity(sessionstate.EndpointSessionSummary{}))
}

// A session is new until it is bound and has a dispatched turn; a paused
// session ranks as paused while its pod is known, and as new once the pod
// left the pool.
func TestSessionClass(t *testing.T) {
	a := newTestAgent(testConfig())
	reg := registryOf(a)
	classOf := func(id string) sessionClass {
		c := &candidate{id: id, sizeBytes: 400}
		a.mu.Lock()
		defer a.mu.Unlock()
		a.classifyLocked(reg, c, now(a))
		return c.class
	}

	require.Equal(t, classNew, classOf("unknown"))
	require.Equal(t, classAdmitted, classOf(""), "anonymous")

	primeFitView(a, "pod-a")
	seed(a, "s1", "pod-a", 300)
	require.Equal(t, classAdmitted, classOf("s1"))
	pause(t, a, "s1")
	require.Equal(t, classPaused, classOf("s1"))
	reg.RemoveEndpoint(podID("pod-a"))
	require.Equal(t, classNew, classOf("s1"), "pod left the pool")

	require.Equal(t, "new", classNew.String())
	require.Equal(t, "admitted", classAdmitted.String())
	require.Equal(t, "paused", classPaused.String())
}

// The accounting rule: a live reservation counts its reserved size on its pod
// and nothing elsewhere, a paused or archived session counts nothing, any
// other session its full size.
func TestFootprint(t *testing.T) {
	a := newTestAgent(testConfig())
	state := sessionstate.SessionState{CommittedTokens: 300, InFlightTokens: 500, TurnsTaken: 1, Status: sessionstate.StatusRunning}
	footprint := func(endpointID string, s sessionstate.SessionState) float64 {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.footprintLocked("s1", endpointID, s, now(a))
	}
	require.Equal(t, float64(500), footprint("p", state), "in-flight estimate above the committed total")
	state.InFlightTokens, state.Status = 0, sessionstate.StatusIdle
	require.Equal(t, float64(300), footprint("p", state))

	archived := state
	archived.Status = sessionstate.StatusArchived
	require.Equal(t, float64(0), footprint("p", archived))

	a.mu.Lock()
	l := a.localLocked("s1")
	l.paused, l.pausedAtTurns = true, 1
	a.mu.Unlock()
	require.Equal(t, float64(0), footprint("p", state))

	a.mu.Lock()
	l.reservedEndpoint, l.reservedTokens, l.reservedUntil, l.reservedAtTurns = "p", 700, now(a).Add(reservationTTL), 1
	a.mu.Unlock()
	require.Equal(t, float64(700), footprint("p", state), "a live reservation counts even while paused")
	require.Equal(t, float64(0), footprint("q", state), "but only on its pod")
	advance(a, reservationTTL)
	require.Equal(t, float64(0), footprint("p", state), "an expired one does not")

	state.TurnsTaken = 2
	require.Equal(t, float64(300), footprint("p", state), "a dispatched turn ends the pause")
}

// Only an admitted session with no turn in flight and no live reservation,
// idle for at least the idle lease since its last response, can give up its
// room.
func TestReclaimable(t *testing.T) {
	a := newTestAgent(testConfig())
	a.idleLease = time.Minute
	idle := func() sessionstate.SessionState {
		return sessionstate.SessionState{CommittedTokens: 300, TurnsTaken: 1, Status: sessionstate.StatusIdle, LastResponseAt: now(a).Add(-time.Minute)}
	}
	reclaimable := func(s sessionstate.SessionState) bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.reclaimableLocked("s1", s, now(a))
	}
	require.True(t, reclaimable(idle()))
	a.idleLease = 2 * time.Minute
	require.False(t, reclaimable(idle()), "inside the lease")
	a.idleLease = 0

	s := idle()
	s.InFlightRequests = 1
	require.False(t, reclaimable(s), "turn in flight")
	s = idle()
	s.TurnsTaken = 0
	require.False(t, reclaimable(s), "never dispatched")
	s = idle()
	s.Status = sessionstate.StatusArchived
	require.False(t, reclaimable(s), "archived sessions already count nothing")

	a.mu.Lock()
	a.localLocked("s1").reservedUntil = now(a).Add(time.Second)
	a.local["s1"].reservedAtTurns = 1
	a.mu.Unlock()
	require.False(t, reclaimable(idle()), "live reservation")

	a.mu.Lock()
	l := a.local["s1"]
	l.reservedUntil, l.paused, l.pausedAtTurns = time.Time{}, true, 1
	a.mu.Unlock()
	require.False(t, reclaimable(idle()), "already paused")
}

// A session with a turn queued is never offered for reclaim, and the spare
// total counts exactly the sessions reclaim may pause.
func TestLoadSkipsQueued(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 300)
	seed(a, "s2", "pod-a", 200)
	advance(a, time.Minute)
	reg := registryOf(a)

	a.mu.Lock()
	defer a.mu.Unlock()
	load := a.loadLocked(reg, podID("pod-a"), 1000, now(a), nil)
	require.Len(t, load.idle, 2)
	require.Equal(t, float64(500), load.occupancy)

	load = a.loadLocked(reg, podID("pod-a"), 1000, now(a), map[string]bool{"s1": true})
	require.Len(t, load.idle, 1)
	require.Equal(t, "s2", load.idle[0].id)
	require.Equal(t, float64(200), load.spare)
}

// Reclaim pauses the longest-idle sessions first and stops as soon as the
// room covers the need; with enough room it pauses nobody.
func TestReclaimLongestIdleFirst(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "old", "pod-a", 300)
	advance(a, time.Minute)
	seed(a, "mid", "pod-a", 300)
	advance(a, time.Minute)
	seed(a, "new", "pod-a", 300)
	advance(a, time.Minute)
	reg := registryOf(a)

	a.mu.Lock()
	load := a.loadLocked(reg, podID("pod-a"), 1000, now(a), nil)
	require.Equal(t, 0, a.reclaimLocked(load, 400, 350))
	require.Equal(t, 2, a.reclaimLocked(load, 100, 650))
	a.mu.Unlock()
	require.True(t, isPaused(a, "old"))
	require.True(t, isPaused(a, "mid"))
	require.False(t, isPaused(a, "new"))
}

// The sweep keeps a live reservation and a live pause, and drops state the
// registry has superseded or forgotten, counting resumed sessions.
func TestSweepDropsSupersededState(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "forgotten", "pod-a", 300)
	pause(t, a, "forgotten")
	advance(a, 2*time.Hour)
	seed(a, "paused", "pod-a", 300)
	seed(a, "resumed", "pod-a", 300)
	pause(t, a, "paused")
	pause(t, a, "resumed")
	a.mu.Lock()
	l := a.localLocked("fresh")
	l.reservedEndpoint, l.reservedTokens, l.reservedUntil = podID("pod-a"), 100, now(a).Add(reservationTTL)
	a.mu.Unlock()

	startTurn(a, "resumed", "pod-a", 400)
	registryOf(a).EvictIdle(now(a), time.Hour)

	a.sweep()
	a.mu.Lock()
	require.Contains(t, a.local, "fresh")
	require.Contains(t, a.local, "paused")
	require.NotContains(t, a.local, "resumed")
	require.NotContains(t, a.local, "forgotten")
	a.mu.Unlock()
	require.Equal(t, float64(1), testutil.ToFloat64(a.metrics.resumes))

	advance(a, reservationTTL)
	a.sweep()
	a.mu.Lock()
	require.NotContains(t, a.local, "fresh")
	a.mu.Unlock()
}

// The snapshot counts bound sessions by state, paused apart from idle, and
// a paused session's tokens are not in its pod's working set.
func TestSnapshot(t *testing.T) {
	a := newTestAgent(testConfig())
	primeFitView(a, "pod-a")
	registryOf(a).UpsertEndpoint(podID("pod-b"), func() int64 { return 2000 })
	startTurn(a, "running", "pod-a", 400)
	seed(a, "idle", "pod-a", 300)
	seed(a, "paused", "pod-a", 200)
	pause(t, a, "paused")
	seed(a, "other", "pod-b", 50)

	snap := a.snapshot()
	require.Equal(t, 1, snap.running)
	require.Equal(t, 2, snap.idle)
	require.Equal(t, 1, snap.paused)
	require.Equal(t, endpointGauge{workingSet: 400, capacity: 1000}, snap.endpoints[podID("pod-a")])
	require.Equal(t, endpointGauge{workingSet: 50, capacity: 2000}, snap.endpoints[podID("pod-b")])
}
