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

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionstate"
)

// A new session is admitted only when a pod has room for its estimate.
func TestNewSessionHeldWhenNoRoom(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 900) // 900 of 1000 used
	primeFitView(a, "pod-a")

	big := makeQueue("s2", now(a), 2000) // estimate 500 > room 100
	require.Nil(t, pick(t, a, big))

	small := makeQueue("s3", now(a), 320) // estimate 80 <= room 100
	require.Equal(t, small, pick(t, a, small))
}

// An admitted session's next turn dispatches even when it pushes its pod
// over the ceiling and no idle session can give up room.
func TestAdmittedBypassesFit(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 950)
	primeFitView(a, "pod-a")

	q := makeQueue("s1", now(a), 8000) // estimate 2000: 1050 over the room
	require.Equal(t, q, pick(t, a, q))
	require.Equal(t, float64(2000), endpointTokens(a, podID("pod-a")))
}

// A picked turn of an admitted session holds its room until PreRequest:
// dispatched, it is no longer queued and not yet in flight, but must not
// look idle.
func TestAdmittedPickReservesRoom(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 300) // room 700
	advance(a, time.Minute)
	primeFitView(a, "pod-a")

	q1 := makeQueue("s1", now(a), 1600) // estimate 400: s1 grows to 400
	require.Equal(t, q1, pick(t, a, q1))
	require.Equal(t, float64(400), endpointTokens(a, podID("pod-a")))

	// s1's turn has left the queue; a new session that would fit only by
	// reclaiming s1 holds.
	q2 := makeQueue("s2", now(a), 3200) // estimate 800 > room 600
	require.Nil(t, pick(t, a, q2))
	require.False(t, isPaused(a, "s1"))
}

// Admitting a new session that does not fit pauses idle sessions past the
// lease, longest idle first, and only as many as the admission needs.
func TestAdmissionReclaimsLeaseExpiredIdleLongestFirst(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 300)
	advance(a, time.Minute)
	seed(a, "s2", "pod-a", 300)
	advance(a, time.Minute)
	seed(a, "s3", "pod-a", 300) // room 100; s1 idle 2m, s2 1m, s3 0
	primeFitView(a, "pod-a")

	q := makeQueue("s4", now(a), 1400) // estimate 350: needs s1's room
	require.Equal(t, q, pick(t, a, q))
	require.True(t, isPaused(a, "s1"))
	require.False(t, isPaused(a, "s2"))
	require.False(t, isPaused(a, "s3"))
	// s2 and s3 plus the 350 reserved for s4.
	require.Equal(t, float64(950), endpointTokens(a, podID("pod-a")))
}

// An idle session inside its lease keeps its room: the admission holds.
func TestAdmissionKeepsIdleWithinLease(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 900) // idle for less than the lease
	primeFitView(a, "pod-a")

	q := makeQueue("s2", now(a), 1600) // estimate 400 > room 100
	require.Nil(t, pick(t, a, q))
	require.False(t, isPaused(a, "s1"))
}

// A new session goes to a pod it fits without pausing anyone before a pod
// where it would fit only by reclaiming, and Score pins it there.
func TestNewSessionPrefersPodWithoutReclaim(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 800) // room 200, 800 reclaimable
	advance(a, time.Minute)
	seed(a, "s2", "pod-b", 600) // room 400
	primeFitView(a, "pod-a", "pod-b")

	q := makeQueue("s3", now(a), 1400) // estimate 350
	require.Equal(t, q, pick(t, a, q))
	require.False(t, isPaused(a, "s1"))
	require.Equal(t, podID("pod-b"), reservedPod(a, "s3"))
}

// An admitted session's turn that pushes its pod over the ceiling pauses
// sessions idle past the lease, longest idle first. A session that just
// finished a turn keeps its room.
func TestAdmittedGrowthReclaimsPastLease(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s2", "pod-a", 250)
	advance(a, 59*time.Second)
	seed(a, "s1", "pod-a", 300)
	advance(a, time.Second)
	seed(a, "s3", "pod-a", 450) // pod full; s1 idle 1s, s2 1m
	primeFitView(a, "pod-a")

	// Estimate 650 grows s3 by 200: pausing s2 (250) covers the growth.
	q := makeQueue("s3", now(a), 2600)
	require.Equal(t, q, pick(t, a, q))
	require.True(t, isPaused(a, "s2"))
	require.False(t, isPaused(a, "s1"))
}

// When every idle session is inside the lease, an admitted session's turn
// still dispatches and pauses nobody; the pod stays over its ceiling.
func TestAdmittedGrowthKeepsSessionsInsideLease(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 550)
	advance(a, time.Second)
	seed(a, "s2", "pod-a", 450) // pod full
	primeFitView(a, "pod-a")

	q := makeQueue("s2", now(a), 2600) // grows s2 by 200
	require.Equal(t, q, pick(t, a, q))
	require.False(t, isPaused(a, "s1"))
}

// A session with a turn queued is about to be active and is never
// reclaimed, even when it is long idle.
func TestQueuedSessionNotReclaimed(t *testing.T) {
	cfg := testConfig()
	cfg.HeadWaitStarvationMs = 1000
	a := newTestAgent(cfg)
	seed(a, "s1", "pod-a", 600) // room 400
	advance(a, time.Minute)
	primeFitView(a, "pod-a")

	q1 := makeQueue("s1", now(a), 400)
	q2 := makeQueue("s2", now(a).Add(-2*time.Second), 2000) // starving, estimate 500
	require.Equal(t, q2, pick(t, a, q1, q2))
	require.False(t, isPaused(a, "s1"))
}

// A session larger than the ceiling fits once every other session on its
// pod can give up room, instead of never.
func TestOversizedSessionFitsWhenPodReclaimable(t *testing.T) {
	cfg := testConfig()
	cfg.UtilThreshold = 0.5 // ceiling 500
	a := newTestAgent(cfg)
	seed(a, "s1", "pod-a", 600)
	seed(a, "s2", "pod-a", 100)
	pause(t, a, "s1")
	primeFitView(a, "pod-a")

	q := makeQueue("s1", now(a), 2400) // estimate 600 > ceiling
	require.Nil(t, pick(t, a, q), "s2 is inside its lease")

	advance(a, time.Minute)
	q = makeQueue("s1", now(a), 2400)
	require.Equal(t, q, pick(t, a, q))
	require.True(t, isPaused(a, "s2"))
}

// A paused session resumes only on its own pod, even when another pod has
// room (strict origin affinity); it goes ahead of new sessions once its pod
// has room again.
func TestPausedResumesOnlyOnItsOwnPod(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 600)
	seed(a, "s2", "pod-a", 500)
	pause(t, a, "s2")
	primeFitView(a, "pod-a", "pod-b")

	// pod-b is empty, but s2 waits for pod-a: room on a is 400 < 500.
	q2 := makeQueue("s2", now(a), 2000)
	require.Nil(t, pick(t, a, q2))

	// s1 is archived, so its blocks count as free; pod-a has room again and
	// s2 beats a fitting newcomer.
	require.True(t, registryOf(a).Archive("s1"))
	qNew := makeQueue("s9", now(a).Add(-10*time.Second), 320) // older, below the starvation deadline
	require.Equal(t, q2, pick(t, a, q2, qNew))
	require.Equal(t, podID("pod-a"), reservedPod(a, "s2"))

	// The dispatched turn ends the pause.
	startTurn(a, "s2", "pod-a", 2000)
	require.False(t, isPaused(a, "s2"))
}

// A paused session whose blocks the registry's capacity eviction released
// stays paused on its pod rather than re-entering as new.
func TestPausedSurvivesRegistryUnbind(t *testing.T) {
	a := newTestAgent(testConfig())
	registryOf(a).UpsertEndpoint(podID("pod-a"), func() int64 { return 1000 })
	primeFitView(a, "pod-b")
	seed(a, "s1", "pod-a", 600)
	pause(t, a, "s1")

	// s2's 1000-token turn pushes pod-a over capacity; the registry evicts
	// idle s1's blocks and unbinds it.
	tr := startTurn(a, "s2", "pod-a", 4000)
	require.Empty(t, stateOf(t, a, "s1").BoundEndpoint)

	// s1 is still ranked as paused: it waits for pod-a although pod-b is
	// empty.
	q := makeQueue("s1", now(a), 2400) // estimate 600
	require.Nil(t, pick(t, a, q))

	finish(a, tr, 300)
	require.Equal(t, q, pick(t, a, q))
	require.Equal(t, podID("pod-a"), reservedPod(a, "s1"))
}

// An admitted session whose blocks the registry's capacity eviction released
// stays admitted while its pod is in the pool: its next turn dispatches
// without a fit check and returns to that pod.
func TestEvictedAdmittedSessionStaysAdmitted(t *testing.T) {
	a := newTestAgent(testConfig())
	registryOf(a).UpsertEndpoint(podID("pod-a"), func() int64 { return 1000 })
	seed(a, "s1", "pod-a", 600)       // acting or idle, inside its lease
	startTurn(a, "s2", "pod-a", 4000) // 1000 running: evicts s1
	state := stateOf(t, a, "s1")
	require.Empty(t, state.BoundEndpoint)
	require.Equal(t, podID("pod-a"), state.LastBoundEndpoint)

	q := makeQueue("s1", now(a), 2400) // pod-a is full
	require.Equal(t, q, pick(t, a, q))
	require.Equal(t, podID("pod-a"), reservedPod(a, "s1"))
}

// An archived session counts nothing on its pod, so its next turn must fit
// there again rather than dispatch as admitted.
func TestArchivedSessionTurnMustFit(t *testing.T) {
	a := newTestAgent(testConfig())
	primeFitView(a, "pod-a")
	seed(a, "s1", "pod-a", 500)
	seed(a, "s2", "pod-a", 600)
	require.True(t, registryOf(a).Archive("s1"))

	q := makeQueue("s1", now(a), 2000) // estimate 500 > room 400
	require.Nil(t, pick(t, a, q))

	require.True(t, registryOf(a).Archive("s2"))
	require.Equal(t, q, pick(t, a, q))
	require.Equal(t, podID("pod-a"), reservedPod(a, "s1"))
}

// A pod the registry was not told about, such as one deleted between
// scheduling and dispatch, is never a placement candidate.
func TestUnregisteredPodIsNotACandidate(t *testing.T) {
	a := newTestAgent(testConfig())
	primeFitView(a, "pod-a")
	seed(a, "s1", "pod-a", 900)
	seed(a, "s0", "gone", 10)

	q := makeQueue("s2", now(a), 2000) // estimate 500 fits only the unknown pod
	require.Nil(t, pick(t, a, q))
}

// A paused session whose pod left the pool re-enters as new.
func TestPausedOnRemovedPodIsNew(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 600)
	pause(t, a, "s1")
	primeFitView(a, "pod-b")
	registryOf(a).RemoveEndpoint(podID("pod-a"))

	q := makeQueue("s1", now(a), 2000) // estimate 600 fits empty pod-b
	require.Equal(t, q, pick(t, a, q))
	require.Equal(t, podID("pod-b"), reservedPod(a, "s1"))
}

// An admission reservation blocks a second admit into the same room until
// it is confirmed or expires.
func TestReservationPreventsDoubleAdmit(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 900) // room 100
	primeFitView(a, "pod-a")

	q2 := makeQueue("s2", now(a), 320) // estimate 80
	q3 := makeQueue("s3", now(a), 320)
	require.Equal(t, q2, pick(t, a, q2, q3))
	// 80 of the 100 are reserved for s2; s3 no longer fits.
	require.Nil(t, pick(t, a, q3))

	// A sweep before the reservation expires keeps it.
	a.sweep()
	require.Equal(t, podID("pod-a"), reservedPod(a, "s2"))
	require.Nil(t, pick(t, a, q3))

	// The reservation expires unconfirmed; the room frees and s3 fits.
	advance(a, reservationTTL)
	require.Equal(t, q3, pick(t, a, q3))
}

// A reservation is charged once the session-state-producer records the turn:
// the turn then counts through the registry.
func TestReservationEndsAtDispatch(t *testing.T) {
	a := newTestAgent(testConfig())
	primeFitView(a, "pod-a")

	q := makeQueue("s1", now(a), 2000)
	require.Equal(t, q, pick(t, a, q))
	require.Equal(t, float64(500), endpointTokens(a, podID("pod-a")))

	tr := startTurn(a, "s1", "pod-a", 1200)
	require.Empty(t, reservedPod(a, "s1"))
	require.Equal(t, float64(300), endpointTokens(a, podID("pod-a")))
	finish(a, tr, 350)
	require.Equal(t, float64(350), endpointTokens(a, podID("pod-a")))
}

// The starvation backstop force-admits a head past the deadline, fit or not.
func TestStarvationForcesAdmission(t *testing.T) {
	cfg := testConfig()
	cfg.HeadWaitStarvationMs = 1000
	a := newTestAgent(cfg)
	seed(a, "s1", "pod-a", 1000)
	primeFitView(a, "pod-a")

	q := makeQueue("s2", now(a).Add(-2*time.Second), 4000)
	require.Equal(t, q, pick(t, a, q))
}

// A paused session force-admitted past the deadline that fits even after
// reclaim takes the room of its pod's sessions idle past the lease.
func TestStarvingPausedSessionReclaimsIdle(t *testing.T) {
	cfg := testConfig()
	cfg.HeadWaitStarvationMs = 1000
	a := newTestAgent(cfg)
	seed(a, "s1", "pod-a", 600)
	advance(a, time.Minute)
	seed(a, "s3", "pod-a", 400) // within the lease
	seed(a, "s2", "pod-a", 700)
	pause(t, a, "s2")
	primeFitView(a, "pod-a")

	// s2 needs 700; room 0 plus s1's reclaimable 600 is not enough.
	q := makeQueue("s2", now(a).Add(-2*time.Second), 2000)
	require.Equal(t, q, pick(t, a, q))
	require.True(t, isPaused(a, "s1"))
	require.False(t, isPaused(a, "s3"))
	// s3 plus the 700 reserved for s2.
	require.Equal(t, float64(1100), endpointTokens(a, podID("pod-a")))
}

// An archived session's blocks are free capacity for admission.
func TestArchivedSessionFreesRoom(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 900)
	primeFitView(a, "pod-a")

	q := makeQueue("s2", now(a), 1600) // estimate 400 > room 100
	require.Nil(t, pick(t, a, q))

	require.True(t, registryOf(a).Archive("s1"))
	require.Equal(t, sessionstate.StatusArchived, stateOf(t, a, "s1").Status)
	require.Equal(t, q, pick(t, a, q))
}

// With no pod known to the registry the gate fails open and only class
// ordering remains.
func TestFailsOpenWithoutFitView(t *testing.T) {
	a := newTestAgent(testConfig())
	q := makeQueue("s1", now(a), 4000000)
	require.Equal(t, q, pick(t, a, q))
}

// Without a session-state registry the gate fails open.
func TestFailsOpenWithoutRegistry(t *testing.T) {
	a := newThunderAgent("test", testConfig())
	q := makeQueue("s1", time.Now(), 4000000)
	require.Equal(t, q, pick(t, a, q))
}

// A head with no session id is anonymous: it passes the gate untracked, even
// when no pod has room.
func TestAnonymousHeadPasses(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 1000) // pod-a full
	primeFitView(a, "pod-a")

	anon := makeFlowQueue("default-flow", "", now(a), 400)
	require.Equal(t, anon, pick(t, a, anon))
}

// The gate reads the session of the queue head, not the queue's fairness ID:
// an admitted session queued under a tenant fairness ID dispatches even when
// its pod is full, where an unknown session would hold.
func TestGateUsesHeadSession(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 1000) // pod-a full
	primeFitView(a, "pod-a")

	q := makeFlowQueue("tenant-a", "s1", now(a), 400)
	require.Equal(t, q, pick(t, a, q))

	unknown := makeFlowQueue("tenant-a", "s9", now(a), 400)
	require.Nil(t, pick(t, a, unknown))
}

// An admitted session with a turn already in flight is sized as that turn
// plus the new one, since PreRequest adds the new estimate to what is in
// flight; its room is reserved at that size.
func TestAdmittedSizeAddsTurnsInFlight(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 300)
	startTurn(a, "s1", "pod-a", 1600) // 400 in flight
	primeFitView(a, "pod-a")

	q := makeQueue("s1", now(a), 1200) // estimate 300: 400 + 300 = 700
	require.Equal(t, q, pick(t, a, q))
	require.Equal(t, float64(700), endpointTokens(a, podID("pod-a")))
}
