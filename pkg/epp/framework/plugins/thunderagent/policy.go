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
	"sort"
	"time"

	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionstate"
)

// reservationTTL bounds an admission reservation whose session never reached
// PreRequest, such as a request cancelled between dispatch and binding.
// Counting a cancelled request as occupancy would be phantom load.
const reservationTTL = 5 * time.Second

// sessionClass ranks a waiting session for admission. Lower dispatches
// first.
type sessionClass int

const (
	// classAdmitted is a session already admitted to a pod: bound to it,
	// at least one turn dispatched, not paused. Its footprint is already
	// counted and finishing its trajectory is what frees capacity, so its
	// turns always dispatch.
	classAdmitted sessionClass = iota
	// classPaused gave its room to another session's turn: its next turn
	// must fit its own pod again, but it outranks never-admitted sessions.
	classPaused
	// classNew is not admitted to any pod: it never dispatched, or its pod
	// left the pool. Admitted only when a pod has room.
	classNew
)

func (c sessionClass) String() string {
	switch c {
	case classAdmitted:
		return "admitted"
	case classPaused:
		return "paused"
	default:
		return "new"
	}
}

// localSession is the plugin's policy state for one session. Both parts are
// tied to the session's TurnsTaken when they were set: once the
// session-state-producer records another dispatched turn, the pause has ended
// and the reservation has been charged. All fields are guarded by
// ThunderAgent.mu.
type localSession struct {
	// paused is set when Pick reclaims the session's room for another turn:
	// the session stops counting against pausedEndpoint, and its next turn
	// must fit that pod again before it dispatches.
	paused         bool
	pausedEndpoint string
	pausedAtTurns  int64

	// Until reservedUntil, a session whose turn Pick has dispatched but
	// PreRequest has not yet charged counts reservedTokens against
	// reservedEndpoint.
	reservedEndpoint string
	reservedTokens   float64
	reservedUntil    time.Time
	reservedAtTurns  int64
}

func (l *localSession) pausedLive(state sessionstate.SessionState) bool {
	return l != nil && l.paused && state.TurnsTaken == l.pausedAtTurns
}

func (l *localSession) reservationLive(state sessionstate.SessionState, now time.Time) bool {
	return l != nil && now.Before(l.reservedUntil) && state.TurnsTaken == l.reservedAtTurns
}

// size is a session's KV footprint in tokens. Each turn resends the whole
// history, so the committed total and the in-flight estimates cover the same
// KV and the larger one is taken.
func size(state sessionstate.SessionState) float64 {
	return float64(max(state.CommittedTokens, state.InFlightTokens))
}

// footprintLocked is the accounting rule for a session bound to endpointID: a
// live reservation counts its reserved size on the reserved pod and nothing
// elsewhere, a paused or archived session counts nothing, everything else
// counts its full size.
func (a *ThunderAgent) footprintLocked(id, endpointID string, state sessionstate.SessionState, now time.Time) float64 {
	l := a.local[id]
	switch {
	case l.reservationLive(state, now):
		if l.reservedEndpoint == endpointID {
			return l.reservedTokens
		}
		return 0
	case l.pausedLive(state), state.Status == sessionstate.StatusArchived:
		return 0
	}
	return size(state)
}

// reclaimableLocked reports whether the session's room can be taken for
// another turn: it is admitted and counted in full, has no turn in flight,
// and has been idle for at least the idle lease since its last response.
func (a *ThunderAgent) reclaimableLocked(id string, state sessionstate.SessionState, now time.Time) bool {
	l := a.local[id]
	return state.TurnsTaken > 0 && state.InFlightRequests == 0 && state.Status != sessionstate.StatusArchived &&
		!l.pausedLive(state) && !l.reservationLive(state, now) &&
		now.Sub(state.LastResponseAt) >= a.idleLease
}

// reclaimable is a session whose room may be taken.
type reclaimable struct {
	id             string
	size           float64
	turns          int64
	lastResponseAt time.Time
}

// podLoad is one pod's working set under the accounting rule.
type podLoad struct {
	id        string
	capacity  float64
	occupancy float64
	// idle lists the sessions whose room can be reclaimed, excluding
	// sessions with a turn queued.
	idle  []reclaimable
	spare float64
}

func (p *podLoad) room(utilThreshold float64) float64 {
	return p.capacity*utilThreshold - p.occupancy
}

// podCapacity returns a pod's KV capacity in tokens: the scraped value, else
// the configured fallback.
func (a *ThunderAgent) podCapacity(summary sessionstate.EndpointSessionSummary) float64 {
	if summary.CapacityTokens > 0 {
		return float64(summary.CapacityTokens)
	}
	return a.capacityTokens
}

// loadLocked computes a pod's load from the registry and the plugin's policy
// state. queued names sessions with a turn waiting, which are never offered
// for reclaim.
func (a *ThunderAgent) loadLocked(reg *sessionstate.SessionStateRegistry, endpointID string, capacity float64,
	now time.Time, queued map[string]bool) podLoad {
	load := podLoad{id: endpointID, capacity: capacity}
	reg.ForEachEndpointSession(endpointID, func(id string, state sessionstate.SessionState) bool {
		load.occupancy += a.footprintLocked(id, endpointID, state, now)
		if !queued[id] && a.reclaimableLocked(id, state, now) {
			load.idle = append(load.idle, reclaimable{id: id, size: size(state), turns: state.TurnsTaken, lastResponseAt: state.LastResponseAt})
			load.spare += size(state)
		}
		return true
	})
	// Reservations onto this pod by sessions bound elsewhere or not at all,
	// such as a new session Pick admitted here.
	for id, l := range a.local {
		if l.reservedEndpoint != endpointID || !now.Before(l.reservedUntil) {
			continue
		}
		state, _ := reg.State(id)
		if state.BoundEndpoint != endpointID && l.reservationLive(state, now) {
			load.occupancy += l.reservedTokens
		}
	}
	return load
}

// endpointsLocked returns every pod the registry knows with its capacity.
func (a *ThunderAgent) endpointsLocked(reg *sessionstate.SessionStateRegistry) map[string]float64 {
	capacities := reg.EndpointCapacities()
	pods := make(map[string]float64, len(capacities))
	for id, capacity := range capacities {
		pods[id] = a.podCapacity(sessionstate.EndpointSessionSummary{CapacityTokens: capacity})
	}
	return pods
}

// capacityLocked returns one pod's capacity, or false if the registry does not
// know the pod.
func (a *ThunderAgent) capacityLocked(reg *sessionstate.SessionStateRegistry, endpointID string) (float64, bool) {
	summary, ok := reg.EndpointSummary(endpointID)
	if !ok {
		return 0, false
	}
	return a.podCapacity(summary), true
}

// reclaimLocked pauses the pod's reclaimable sessions, longest idle first,
// until room covers need, and returns how many it paused.
func (a *ThunderAgent) reclaimLocked(load podLoad, room, need float64) int {
	if room >= need {
		return 0
	}
	idle := append([]reclaimable(nil), load.idle...)
	sort.Slice(idle, func(i, j int) bool { return idle[i].lastResponseAt.Before(idle[j].lastResponseAt) })
	paused := 0
	for _, s := range idle {
		if room >= need {
			break
		}
		l := a.localLocked(s.id)
		l.paused, l.pausedEndpoint, l.pausedAtTurns = true, load.id, s.turns
		room += s.size
		paused++
	}
	return paused
}

func (a *ThunderAgent) localLocked(id string) *localSession {
	l, ok := a.local[id]
	if !ok {
		l = &localSession{}
		a.local[id] = l
	}
	return l
}

// refreshLocked ends a pause and a reservation that the session's progress
// has superseded, and drops the entry once neither is live. It reports
// whether the session resumed from a pause.
func (a *ThunderAgent) refreshLocked(id string, state sessionstate.SessionState, tracked bool, now time.Time) bool {
	l, ok := a.local[id]
	if !ok {
		return false
	}
	resumed := false
	if l.paused && (!tracked || state.TurnsTaken != l.pausedAtTurns) {
		resumed = tracked
		l.paused, l.pausedEndpoint = false, ""
	}
	if !l.reservationLive(state, now) {
		l.reservedEndpoint, l.reservedTokens, l.reservedUntil = "", 0, time.Time{}
	}
	if !l.paused && l.reservedUntil.IsZero() {
		delete(a.local, id)
	}
	return resumed
}

// sweep drops policy state the registry has superseded or forgotten.
func (a *ThunderAgent) sweep() {
	reg := a.sessions()
	if reg == nil {
		return
	}
	now := a.clock.Now()
	a.mu.Lock()
	resumed := 0
	for id := range a.local {
		state, tracked := reg.State(id)
		if a.refreshLocked(id, state, tracked, now) {
			resumed++
		}
	}
	a.mu.Unlock()
	a.metrics.resumes.Add(float64(resumed))
}

// estimateTokens converts a request body size to a token estimate. The body
// also holds the JSON envelope, tool schemas and images, so the estimate is
// above the prompt token count.
// TODO(#3221): use the request's token count once it is available before flow
// control.
func estimateTokens(sizeBytes int) int64 {
	if sizeBytes <= 0 {
		return 0
	}
	return int64(sizeBytes / fwkrh.BytesPerToken)
}

// gaugeSnapshot is the state the metrics collector reports.
type gaugeSnapshot struct {
	endpoints             map[string]endpointGauge
	running, idle, paused int
}

type endpointGauge struct {
	workingSet float64
	capacity   float64
}

// snapshot returns the values to report. Sessions are counted when bound to a
// pod or paused by the plugin.
func (a *ThunderAgent) snapshot() gaugeSnapshot {
	snap := gaugeSnapshot{endpoints: map[string]endpointGauge{}}
	reg := a.sessions()
	if reg == nil {
		return snap
	}
	now := a.clock.Now()
	a.mu.Lock()
	defer a.mu.Unlock()

	counted := map[string]bool{}
	for id, capacity := range a.endpointsLocked(reg) {
		load := a.loadLocked(reg, id, capacity, now, nil)
		snap.endpoints[id] = endpointGauge{workingSet: load.occupancy, capacity: capacity}
		reg.ForEachEndpointSession(id, func(sid string, state sessionstate.SessionState) bool {
			counted[sid] = true
			switch {
			case a.local[sid].pausedLive(state):
				snap.paused++
			case state.InFlightRequests > 0:
				snap.running++
			default:
				snap.idle++
			}
			return true
		})
	}
	for id, l := range a.local {
		if counted[id] || !l.paused {
			continue
		}
		if state, ok := reg.State(id); ok && l.pausedLive(state) {
			snap.paused++
		}
	}
	return snap
}
