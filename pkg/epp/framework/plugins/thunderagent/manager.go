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
	"sync"
	"time"
)

// bytesPerToken converts a request body size to an estimated prompt token
// count.
const bytesPerToken = 4.0

// maintenanceInterval bounds how often the full-table maintenance (TTL
// eviction, stale endpoint cleanup) runs.
const maintenanceInterval = time.Second

// endpointStaleAfter drops a pod entry that holds no sessions and has not been
// seen for this long.
const endpointStaleAfter = 5 * time.Second

// reservationTTL bounds an admission reservation whose session never reached
// PreRequest, such as a request cancelled between dispatch and binding.
// Counting a cancelled request as occupancy would be phantom load.
const reservationTTL = 5 * time.Second

// session is one agent trajectory, identified by the request FairnessID.
// All fields are guarded by sessionManager.mu.
type session struct {
	// endpoint the session is bound to; nil before the first dispatch and
	// after the endpoint leaves the pool.
	endpoint *endpointState
	// committedTokens is usage.total_tokens of the last completed turn.
	committedTokens int64
	// inflightTokens is the estimate of the turn currently being processed.
	// A session is assumed to have at most one request in flight.
	inflightTokens int64
	lastResponseAt time.Time
	lastActivity   time.Time
	turnCount      int64
	// paused is set when Pick reclaims the session's room for another turn:
	// the session stops counting against its endpoint, and its next turn
	// must pass the fit check again before it dispatches.
	paused bool
	// Until reservedUntil, a session whose turn Pick has dispatched but
	// PreRequest has not yet charged counts reservedTokens against its
	// endpoint.
	reservedTokens float64
	reservedUntil  time.Time
}

// size is the session's KV footprint in tokens.
func (s *session) size() float64 {
	if f := float64(s.inflightTokens); f > float64(s.committedTokens) {
		return f
	}
	return float64(s.committedTokens)
}

// class ranks the session for admission. The order of the checks matters: a
// session whose pod left the pool re-enters as new even if it was paused.
func (s *session) class() SessionClass {
	if s.endpoint == nil || s.turnCount == 0 {
		return ClassNew
	}
	if s.paused {
		return ClassPaused
	}
	return ClassAdmitted
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

// pausable reports whether the session can give up its room at all: it is
// admitted and counted in full, with no turn in flight and no live
// reservation. Whether it should is the AdmissionPolicy's decision.
func (s *session) pausable(now time.Time) bool {
	return s.turnCount > 0 && !s.paused && !now.Before(s.reservedUntil) && s.inflightTokens == 0
}

// endpointState is the plugin's own record of one endpoint.
type endpointState struct {
	id       string
	capacity float64
	// sessions are the sessions bound to this pod.
	sessions map[string]*session
	// updatedAt is the last time this pod was seen.
	updatedAt time.Time
}

// occupancy is the endpoint's working set under the accounting rule.
func (p *endpointState) occupancy(now time.Time) float64 {
	var total float64
	for _, s := range p.sessions {
		total += s.footprint(now)
	}
	return total
}

// idleSessions returns the endpoint's sessions that can give up their room,
// by id. Sessions with a turn queued are skipped: they are about to be
// active.
func (p *endpointState) idleSessions(now time.Time, queued map[string]bool) map[string]*session {
	idle := make(map[string]*session)
	for id, s := range p.sessions {
		if s.pausable(now) && !queued[id] {
			idle[id] = s
		}
	}
	return idle
}

// sessionManager is the ledger shared by all of the thunder agent plugin's hooks/
type sessionManager struct {
	// mu guards everything below, including all session and endpointState
	// fields. The Locked suffix and the session / endpointState helpers all
	// assume the caller holds it.
	mu        sync.Mutex
	sessions  map[string]*session
	endpoints map[string]*endpointState

	ttl             time.Duration
	lastMaintenance time.Time
	// forget, when set, is told of every session dropped from the ledger.
	forget func(id string)
}

func newSessionManager(cfg Config) *sessionManager {
	return &sessionManager{
		sessions:  make(map[string]*session),
		endpoints: make(map[string]*endpointState),
		ttl:       time.Duration(cfg.EvictionTTLSeconds * float64(time.Second)),
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
func (m *sessionManager) bindLocked(id string, ep *endpointState) *session {
	s, ok := m.sessions[id]
	if !ok {
		s = &session{}
		m.sessions[id] = s
	}
	if s.endpoint != ep {
		if s.endpoint != nil {
			delete(s.endpoint.sessions, id)
		}
		s.endpoint = ep
		ep.sessions[id] = s
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
	if m.forget != nil {
		m.forget(id)
	}
}

// estimateTokens converts a request body size to a token estimate.
func estimateTokens(sizeBytes int) int64 {
	if sizeBytes <= 0 {
		return 0
	}
	return int64(float64(sizeBytes) / bytesPerToken)
}

// gaugeSnapshot is the ledger state the metrics collector reports.
type gaugeSnapshot struct {
	endpoints             map[string]endpointGauge
	running, idle, paused int
}

type endpointGauge struct {
	workingSet float64
	capacity   float64
}

// maintainLocked is the housekeeping pass, run at most once per
// maintenanceInterval by whichever hook holds the lock: drop sessions idle
// past the TTL and drop empty stale endpoints.
func (m *sessionManager) maintainLocked(now time.Time) {
	if now.Sub(m.lastMaintenance) < maintenanceInterval {
		return
	}
	m.lastMaintenance = now

	for id, s := range m.sessions {
		// Keep live reservations: a session Pick admitted has no activity yet,
		// so once its reservation expires unconfirmed the TTL below drops it.
		if now.Before(s.reservedUntil) {
			continue
		}
		if s.inflightTokens > 0 {
			continue
		}
		if now.Sub(s.lastActivity) <= m.ttl {
			continue
		}
		m.removeLocked(id)
	}
	for id, p := range m.endpoints {
		if len(p.sessions) == 0 && now.Sub(p.updatedAt) > endpointStaleAfter {
			delete(m.endpoints, id)
		}
	}
}

// snapshot runs due maintenance, so a scrape keeps the ledger current without
// traffic, and returns the values to report.
func (m *sessionManager) snapshot(now time.Time) gaugeSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maintainLocked(now)

	snap := gaugeSnapshot{endpoints: make(map[string]endpointGauge, len(m.endpoints))}
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
