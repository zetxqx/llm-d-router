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
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The pause sweep is upstream _pause_until_safe: while the undecayed working
// set exceeds the ceiling, idle programs are paused smallest first.
func TestPauseSweepPausesSmallestIdleFirst(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	// 100 + 200 + 900 = 1200 tokens on a 900-token ceiling. Pausing smallest
	// first drops 100 (1100, still over) then 200 (900, at the ceiling); the
	// 900-token program survives.
	seedProgram(t, a, "small", sched[0], 100)
	seedProgram(t, a, "medium", sched[0], 200)
	seedProgram(t, a, "large", sched[0], 900)

	a.Saturation(context.Background(), dlEndpoints("pod1"))

	assert.True(t, isPaused(a, "small"))
	assert.True(t, isPaused(a, "medium"))
	assert.False(t, isPaused(a, "large"))
	assert.Equal(t, 2.0, testutil.ToFloat64(a.metrics.pauses))

	snap := snapshotOf(a, "default/pod1")
	require.NotNil(t, snap)
	assert.Equal(t, 900.0, snap.tokens, "the fit view reflects the post-pause working set")
	assert.InDelta(t, 0.0, snap.room, 1e-9)
}

// Upstream pauses any ACTING program regardless of how long it has been idle.
func TestPauseSweepIgnoresIdleAge(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "just-finished", sched[0], 1200) // idle for microseconds
	a.Saturation(context.Background(), dlEndpoints("pod1"))

	assert.True(t, isPaused(a, "just-finished"))
	a.table.mu.Lock()
	origin := a.table.programs["just-finished"].podName
	a.table.mu.Unlock()
	assert.Equal(t, "default/pod1", origin, "a paused program keeps its origin pod for sticky resume")
}

// When no idle program is left, upstream marks in-flight programs for pause
// at the end of their turn. Its loop condition does not account for marks, so
// every in-flight program on the pod gets marked. A mark matures when the
// turn's response completes.
func TestPauseSweepMarksAllInflightWhenNoIdle(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "a", sched[0], 600)
	seedProgram(t, a, "b", sched[0], 500)
	reqA := inflightRequest(t, a, "a", sched[0], 400) // 100 in flight, footprint stays 600
	inflightRequest(t, a, "b", sched[0], 400)         // footprint 500; 1100 > 900

	a.Saturation(context.Background(), dlEndpoints("pod1"))

	assert.False(t, isPaused(a, "a"), "a program with a request in flight is never paused outright")
	assert.False(t, isPaused(a, "b"))
	assert.True(t, isMarked(a, "a"))
	assert.True(t, isMarked(a, "b"), "upstream marks every in-flight program before it breaks")
	assert.Equal(t, 0.0, testutil.ToFloat64(a.metrics.pauses))
	assert.Equal(t, 1100.0, snapshotOf(a, "default/pod1").tokens, "marked programs still count")

	a.ResponseBody(context.Background(), reqA, endOfStream(650, 600), nil)
	assert.True(t, isPaused(a, "a"), "the mark matures at end of stream")
	assert.False(t, isMarked(a, "a"))
	assert.True(t, isMarked(a, "b"), "b is still running")
	assert.Equal(t, 1.0, testutil.ToFloat64(a.metrics.pauses))
}

func TestPauseSweepThrottledByInterval(t *testing.T) {
	cfg := testConfig()
	cfg.PauseSweepSeconds = 3600
	a := newTestAgent(cfg)
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "over", sched[0], 1200)
	a.Saturation(context.Background(), dlEndpoints("pod1"))
	a.Saturation(context.Background(), dlEndpoints("pod1"))

	assert.False(t, isPaused(a, "over"), "the first sweep runs one interval after the pod appears")
	assert.Equal(t, 1200.0, snapshotOf(a, "default/pod1").tokens, "the fit view still refreshes every call")

	// Backdate the last sweep past the interval: the next call sweeps.
	a.table.mu.Lock()
	a.table.snapshot["default/pod1"].sweptAt = time.Now().Add(-2 * time.Hour)
	a.table.mu.Unlock()
	a.Saturation(context.Background(), dlEndpoints("pod1"))
	assert.True(t, isPaused(a, "over"))
}

// A paused program's next turn is fit-checked (no bypass) with the larger of
// its committed tokens and the new turn's estimate, and its origin pod stays
// its sticky target while no reservation says otherwise.
func TestPausedProgramIsFitChecked(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "squatter", sched[0], 600)
	seedProgram(t, a, "runner", sched[0], 700)
	a.Saturation(context.Background(), dlEndpoints("pod1"))
	require.True(t, isPaused(a, "squatter"), "the smaller idle program is paused")
	require.False(t, isPaused(a, "runner"))

	a.table.mu.Lock()
	class, tokens := a.table.classAndTokens("squatter")
	a.table.mu.Unlock()
	assert.Equal(t, classPaused, class)
	assert.Equal(t, 600.0, tokens)

	// Held while the survivor leaves no room (700 + 600 > 900).
	q := makeQueueWithBytes("squatter", 1, time.Now(), 600*4)
	picked, err := a.Pick(context.Background(), bandOf(q))
	require.NoError(t, err)
	assert.Nil(t, picked, "a paused program's next turn is fit-checked, not bypassed")

	// Still pinned to its origin for placement.
	scores := a.Score(context.Background(), newRequest("squatter", 600*4), sched)
	assert.Equal(t, 1.0, scores[sched[0]], "a paused program keeps its sticky origin")
}

func TestPausedFitUsesLargerOfCommittedAndEstimate(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "grower", sched[0], 100)
	forcePause(a, "grower")
	primeFitView(a, "pod1") // empty pod: room 900

	big := makeQueueWithBytes("grower", 1, time.Now(), 4000) // new turn estimates 1000 tokens
	picked, err := a.Pick(context.Background(), bandOf(big))
	require.NoError(t, err)
	assert.Nil(t, picked, "the new turn's size, not the old committed 100, is what must fit")

	small := makeQueueWithBytes("grower", 1, time.Now(), 200) // estimates 50, committed 100 is the floor
	picked, err = a.Pick(context.Background(), bandOf(small))
	require.NoError(t, err)
	require.Same(t, small, picked)
	res, ok := reservationOf(a, "grower")
	require.True(t, ok)
	assert.Equal(t, 100.0, res.tokens, "committed tokens are the floor of the fit size")
}

// Sticky if it fits: a paused program resumes onto its origin pod when the
// origin has room, even when another pod has more.
func TestPausedPrefersOriginPodWhenItFits(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1", "pod2")

	seedProgram(t, a, "veteran", sched[0], 300)
	seedProgram(t, a, "runner", sched[0], 500) // pod1 room 400 once veteran is paused
	forcePause(a, "veteran")
	primeFitView(a, "pod1", "pod2") // pod2 room 900

	q := makeQueueWithBytes("veteran", 1, time.Now(), 300*4)
	picked, err := a.Pick(context.Background(), bandOf(q))
	require.NoError(t, err)
	require.Same(t, q, picked)
	res, ok := reservationOf(a, "veteran")
	require.True(t, ok)
	assert.Equal(t, "default/pod1", res.pod, "origin wins while it fits")

	scores := a.Score(context.Background(), newRequest("veteran", 300*4), sched)
	assert.Equal(t, 1.0, scores[sched[0]])
	assert.Equal(t, 0.0, scores[sched[1]])
}

func TestPausedFallsBackToMostRoomWhenOriginIsFull(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1", "pod2")

	seedProgram(t, a, "veteran", sched[0], 300)
	seedProgram(t, a, "runner", sched[0], 700) // pod1 room 200 once veteran is paused
	forcePause(a, "veteran")
	primeFitView(a, "pod1", "pod2")

	q := makeQueueWithBytes("veteran", 1, time.Now(), 300*4)
	picked, err := a.Pick(context.Background(), bandOf(q))
	require.NoError(t, err)
	require.Same(t, q, picked)
	res, ok := reservationOf(a, "veteran")
	require.True(t, ok)
	assert.Equal(t, "default/pod2", res.pod)

	scores := a.Score(context.Background(), newRequest("veteran", 300*4), sched)
	assert.Equal(t, 1.0, scores[sched[1]], "the reservation overrides the sticky origin")
	assert.Equal(t, 0.0, scores[sched[0]])

	req := newRequest("veteran", 300*4)
	require.NoError(t, a.PreRequest(context.Background(), req, schedulingResultFor(sched[1])))
	assert.False(t, isPaused(a, "veteran"))
	assert.Equal(t, 1.0, testutil.ToFloat64(a.metrics.rebinds), "resuming elsewhere is a rebind")
}

// A paused program outranks a never-admitted one when both fit, regardless of
// size and enqueue order (upstream's REASONING group precedes NEW).
func TestPick_PausedBeatsNewcomer(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "veteran", sched[0], 400)
	seedProgram(t, a, "runner", sched[0], 600) // 1000 > 900 ceiling
	a.Saturation(context.Background(), dlEndpoints("pod1"))
	require.True(t, isPaused(a, "veteran"))

	// Free the pod so both candidates fit.
	seedProgram(t, a, "runner", sched[0], 100)
	primeFitView(a, "pod1")

	newcomer := makeQueueWithBytes("newcomer", 1, time.Now().Add(-time.Second), 300*4) // smaller and older
	veteran := makeQueueWithBytes("veteran", 1, time.Now(), 400*4)
	picked, err := a.Pick(context.Background(), bandOf(newcomer, veteran))
	require.NoError(t, err)
	assert.Same(t, veteran, picked, "paused outranks new even when the newcomer is smaller and waited longer")
}

// Class priority applies among fitting candidates only: a paused program that
// does not fit must not block a fitting newcomer (work conservation, same as
// upstream's resume selection skipping non-fitting candidates).
func TestPick_NonFittingPausedDoesNotBlockFittingNewcomer(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")

	seedProgram(t, a, "veteran", sched[0], 800)
	seedProgram(t, a, "runner", sched[0], 700) // 1500 > 900 ceiling
	a.Saturation(context.Background(), dlEndpoints("pod1"))
	require.True(t, isPaused(a, "runner"), "the smaller idle program is paused first")
	require.False(t, isPaused(a, "veteran"))

	// Room is 900 - 800 = 100: the 700-token runner cannot fit, a 50-token
	// newcomer can.
	newcomer := makeQueueWithBytes("newcomer", 1, time.Now(), 50*4)
	runner := makeQueueWithBytes("runner", 1, time.Now(), 700*4)
	picked, err := a.Pick(context.Background(), bandOf(runner, newcomer))
	require.NoError(t, err)
	assert.Same(t, newcomer, picked)
}

// End to end against upstream's semantics on one pod: the sweep pauses the
// smallest idle program when the pod is over capacity; its next turn is held
// while running programs fill the room and admitted once they finish; a new
// program that fits at the same time is admitted after it.
func TestUpstreamSemanticsOnOnePod(t *testing.T) {
	a := newTestAgent(testConfig())
	sched := newTestEndpoints("pod1")
	pod := dlEndpoints("pod1")

	seedProgram(t, a, "a", sched[0], 300)
	seedProgram(t, a, "b", sched[0], 350)
	seedProgram(t, a, "c", sched[0], 400) // 1050 > 900

	a.Saturation(context.Background(), pod)
	require.True(t, isPaused(a, "a"), "smallest idle program is paused")
	require.False(t, isPaused(a, "b"))
	require.False(t, isPaused(a, "c"))

	// b and c start their next turns: 750 running, room 150. Request sizes
	// match the 4.0 bytes-per-token seed so the estimator stays put.
	reqB := inflightRequest(t, a, "b", sched[0], 800)
	reqC := inflightRequest(t, a, "c", sched[0], 800)
	a.Saturation(context.Background(), pod)

	aQueue := makeQueueWithBytes("a", 1, time.Now(), 300*4)
	newQueue := makeQueueWithBytes("new", 1, time.Now(), 100*4)
	picked, err := a.Pick(context.Background(), bandOf(aQueue, newQueue))
	require.NoError(t, err)
	assert.Same(t, newQueue, picked, "the 100-token newcomer fits the 150 room, the 300-token paused program does not")
	a.table.mu.Lock()
	delete(a.table.pending, "new") // pretend the newcomer was never bound, to keep the room for the next check
	a.table.mu.Unlock()

	// b and c finish smaller (context compaction): room opens for both.
	a.ResponseBody(context.Background(), reqB, endOfStream(200, 200), nil)
	a.ResponseBody(context.Background(), reqC, endOfStream(200, 200), nil)
	a.Saturation(context.Background(), pod)

	picked, err = a.Pick(context.Background(), bandOf(newQueue, aQueue))
	require.NoError(t, err)
	assert.Same(t, aQueue, picked, "the paused program resumes before the newcomer once both fit")
	picked, err = a.Pick(context.Background(), bandOf(newQueue))
	require.NoError(t, err)
	assert.Same(t, newQueue, picked, "and the newcomer follows")
}
