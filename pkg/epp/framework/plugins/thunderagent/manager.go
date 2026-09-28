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
	"sync"
	"time"
)

// Estimator constants: the request-body-bytes to prompt-tokens ratio starts
// at initialBytesPerToken and is refined from observed usage with momentum,
// clamped against outlier samples.
const (
	initialBytesPerToken = 4.0
	minBytesPerToken     = 1.0
	maxBytesPerToken     = 64.0
	estimatorMomentum    = 0.8
)

// maintenanceInterval bounds how often the full-table maintenance (TTL
// eviction, gauge refresh) runs. Maintenance piggybacks on the Saturation
// hook, which the flow controller calls every dispatch cycle; the plugin has
// no background goroutine.
const maintenanceInterval = time.Second

// endpointStaleAfter drops a pod that has not been reported for this long;
// its sessions re-enter as new.
const endpointStaleAfter = 5 * time.Second

// minSessionTTL is the shortest idle time after which a session's state is
// dropped. Past the idle lease an idle session's room is already reclaimable,
// so the TTL only bounds memory; it is stretched to twice the starvation
// deadline so a held session is never dropped while it waits.
const minSessionTTL = time.Hour

// reservationTTL bounds an admission reservation whose session never reached
// PreRequest, such as a request cancelled between dispatch and binding.
// Counting a cancelled request as occupancy would be phantom load.
const reservationTTL = 5 * time.Second

// sessionClass ranks a waiting session for admission. Lower dispatches
// first.
type sessionClass int

const (
	// classReasoning is a bound, unpaused session: its footprint is already
	// counted and finishing its trajectory is what frees capacity, so its
	// turns always dispatch.
	classReasoning sessionClass = iota
	// classPaused gave its room to another session's turn: its next turn
	// must fit its own pod again, but it outranks never-admitted sessions.
	classPaused
	// classNew has no KV anywhere yet: admitted only when a pod has room.
	classNew
)

func (c sessionClass) String() string {
	switch c {
	case classReasoning:
		return "reasoning"
	case classPaused:
		return "paused"
	default:
		return "new"
	}
}

// session is one agent trajectory, identified by the request FairnessID.
// All fields are guarded by sessionManager.mu.
type session struct {
	// endpoint the session is bound to; nil before the first dispatch and
	// after the endpoint leaves the pool.
	endpoint *endpointState
	// committedTokens is usage.total_tokens of the last completed turn.
	// Each turn resends the whole history, so the latest value replaces the
	// previous one; it is the session's current KV footprint.
	committedTokens int64
	// inflightTokens is the sum of the estimates of this session's turns
	// currently being processed.
	inflightTokens int64
	// lastResponseAt is when the last turn completed: the start of the
	// current idle period, for the lease and the TTL.
	lastResponseAt time.Time
	// dispatched is set by the first PreRequest.
	dispatched bool
	// paused is set when Pick reclaims the session's room for another
	// turn: the session stops counting against its endpoint, and its next
	// turn must pass the fit check again before it dispatches.
	paused bool
	// Until reservedUntil, a session Pick has admitted but PreRequest has
	// not yet bound counts reservedTokens against its endpoint, so the same
	// room is not handed out twice.
	reservedTokens float64
	reservedUntil  time.Time
}

// size is the session's KV footprint in tokens: the larger of the
// in-flight estimate and the committed total. The two describe the same KV,
// because a turn's prefill covers the whole previous history.
func (s *session) size() float64 {
	if f := float64(s.inflightTokens); f > float64(s.committedTokens) {
		return f
	}
	return float64(s.committedTokens)
}

// class ranks the session for admission. The order of the checks matters: a
// session whose pod left the pool re-enters as new even if it was paused.
func (s *session) class() sessionClass {
	if s.endpoint == nil || !s.dispatched {
		return classNew
	}
	if s.paused {
		return classPaused
	}
	return classReasoning
}

// footprint is the accounting rule: an unexpired reservation counts its
// reserved size, a paused session counts nothing, everything else counts
// its full footprint.
func (s *session) footprint(now time.Time) float64 {
	switch {
	case now.Before(s.reservedUntil):
		return s.reservedTokens
	case s.paused:
		return 0
	}
	return s.size()
}

// reclaimable reports whether the session's room can be taken for another
// turn: it is bound and counted in full, has no turn in flight, and has been
// idle for at least minIdle since its last response.
func (s *session) reclaimable(now time.Time, minIdle time.Duration) bool {
	return s.dispatched && !s.paused && !now.Before(s.reservedUntil) && s.inflightTokens == 0 &&
		now.Sub(s.lastResponseAt) >= minIdle
}

// endpointState is the plugin's own record of one endpoint (one pod),
// keyed by its metadata ID. The framework's endpoint objects are read-only
// inputs; everything the plugin tracks per endpoint lives here.
type endpointState struct {
	id       string
	capacity float64
	// sessions are the sessions bound to this pod.
	sessions map[string]*session
	// updatedAt is the last time this pod was seen.
	updatedAt time.Time
}

// occupancy sums the endpoint's working set under the accounting rule.
func (p *endpointState) occupancy(now time.Time) float64 {
	total := 0.0
	for _, s := range p.sessions {
		total += s.footprint(now)
	}
	return total
}

// idleSessions returns the endpoint's sessions whose room can be reclaimed.
// Sessions with a turn queued are skipped: they are about to be active.
func (p *endpointState) idleSessions(now time.Time, minIdle time.Duration, queued map[string]bool) []*session {
	var idle []*session
	for id, s := range p.sessions {
		if s.reclaimable(now, minIdle) && !queued[id] {
			idle = append(idle, s)
		}
	}
	return idle
}

// reclaimableTokens sums the footprints reclaim could free with the same
// arguments.
func (p *endpointState) reclaimableTokens(now time.Time, minIdle time.Duration, queued map[string]bool) float64 {
	total := 0.0
	for _, s := range p.idleSessions(now, minIdle, queued) {
		total += s.size()
	}
	return total
}

// reclaim pauses the endpoint's reclaimable sessions, longest idle first,
// until room covers need, and returns how many it paused.
func (p *endpointState) reclaim(now time.Time, minIdle time.Duration, queued map[string]bool, room, need float64) int {
	if room >= need {
		return 0
	}
	idle := p.idleSessions(now, minIdle, queued)
	sort.Slice(idle, func(i, j int) bool { return idle[i].lastResponseAt.Before(idle[j].lastResponseAt) })
	paused := 0
	for _, s := range idle {
		if room >= need {
			break
		}
		s.paused = true
		room += s.size()
		paused++
	}
	return paused
}

// sessionManager is the ledger shared by all of the plugin's hooks: a
// global by-id index and per-endpoint ownership of the same sessions.
type sessionManager struct {
	// mu guards everything below, including all session and endpointState
	// fields. The Locked suffix and the session / endpointState helpers all
	// assume the caller holds it; stated here once instead of on every
	// function.
	mu        sync.Mutex
	sessions  map[string]*session
	endpoints map[string]*endpointState

	bytesPerToken   float64
	ttl             time.Duration
	lastMaintenance time.Time
}

func newSessionManager(cfg Config) *sessionManager {
	starvation := time.Duration(cfg.HeadWaitStarvationMs * float64(time.Millisecond))
	return &sessionManager{
		sessions:      make(map[string]*session),
		endpoints:     make(map[string]*endpointState),
		bytesPerToken: initialBytesPerToken,
		ttl:           max(minSessionTTL, 2*starvation),
	}
}

// ensureEndpointLocked returns the ledger entry for an endpoint, creating
// it on first sight and refreshing its capacity.
func (m *sessionManager) ensureEndpointLocked(id string, capacity float64, now time.Time) *endpointState {
	p, ok := m.endpoints[id]
	if !ok {
		p = &endpointState{id: id, sessions: make(map[string]*session)}
		m.endpoints[id] = p
	}
	p.capacity = capacity
	p.updatedAt = now
	return p
}

// bindLocked returns the session for id bound to the given endpoint,
// creating the session on first sight and moving it if it was bound
// elsewhere.
func (m *sessionManager) bindLocked(id string, pod *endpointState) *session {
	s, ok := m.sessions[id]
	if !ok {
		s = &session{}
		m.sessions[id] = s
	}
	if s.endpoint != pod {
		if s.endpoint != nil {
			delete(s.endpoint.sessions, id)
		}
		s.endpoint = pod
		pod.sessions[id] = s
	}
	return s
}

// removeLocked drops a session from the ledger and from its endpoint.
func (m *sessionManager) removeLocked(id string) {
	s, ok := m.sessions[id]
	if !ok {
		return
	}
	if s.endpoint != nil {
		delete(s.endpoint.sessions, id)
	}
	delete(m.sessions, id)
}

// estimateTokensLocked converts a request body size to a token estimate
// using the running bytes-per-token ratio. The estimate is at least one
// token, so a session's inflightTokens is nonzero exactly while it has a turn
// in flight.
func (m *sessionManager) estimateTokensLocked(sizeBytes int) int64 {
	return max(int64(float64(sizeBytes)/m.bytesPerToken), 1)
}

// refineEstimatorLocked updates the bytes-per-token ratio from an observed
// request size and its usage-reported prompt tokens.
func (m *sessionManager) refineEstimatorLocked(sizeBytes, promptTokens int) {
	if promptTokens <= 0 || sizeBytes <= 0 {
		return
	}
	sample := float64(sizeBytes) / float64(promptTokens)
	ratio := estimatorMomentum*m.bytesPerToken + (1-estimatorMomentum)*sample
	m.bytesPerToken = min(max(ratio, minBytesPerToken), maxBytesPerToken)
}

// gaugeSnapshot carries the values maintainLocked computed, to be published
// to Prometheus outside the lock.
type gaugeSnapshot struct {
	endpoints             map[string]endpointGauge
	running, idle, paused int
}

type endpointGauge struct {
	workingSet float64
	capacity   float64
}

// maintainLocked is the housekeeping pass, run at most once per
// maintenanceInterval: drop new sessions whose reservation expired
// unconfirmed, drop sessions idle past the TTL (walking the global index, so
// unbound sessions are covered), and snapshot the gauges. Returns nil when
// rate-limited.
func (m *sessionManager) maintainLocked(now time.Time) *gaugeSnapshot {
	if now.Sub(m.lastMaintenance) < maintenanceInterval {
		return nil
	}
	m.lastMaintenance = now

	for id, s := range m.sessions {
		if now.Before(s.reservedUntil) || s.inflightTokens > 0 {
			continue
		}
		if !s.dispatched || now.Sub(s.lastResponseAt) > m.ttl {
			m.removeLocked(id)
		}
	}

	snap := &gaugeSnapshot{endpoints: make(map[string]endpointGauge, len(m.endpoints))}
	for _, s := range m.sessions {
		switch {
		case s.paused:
			snap.paused++
		case s.inflightTokens > 0:
			snap.running++
		default:
			snap.idle++
		}
	}
	for id, p := range m.endpoints {
		snap.endpoints[id] = endpointGauge{workingSet: p.occupancy(now), capacity: p.capacity}
	}
	return snap
}
