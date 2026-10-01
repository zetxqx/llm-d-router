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
	"math"
	"sort"
	"time"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// The gate's two policy decisions are pluggable: when an idle session's next
// turn will arrive (NextTurnPredictor) and, from that, which waiting session
// is admitted and which idle sessions give up their room (AdmissionPolicy).
// Implementations are ordinary plugins that thunder-agent references by name
// (nextTurnPredictor, admissionPolicy); without a reference it uses the
// built-in lease predictor and default policy below. The mechanism stays in
// the gate: the ledger and its accounting rule, reservations, which sessions
// can be paused at all (no turn in flight or queued, no live reservation),
// admitted sessions' turns always dispatching, strict origin affinity, and
// the starvation backstop. Every method is called under the ledger lock, from
// Pick about once per millisecond while requests wait, so it must be fast and
// must not call back into the plugin.

// SessionClass ranks a waiting session for admission. Lower dispatches
// first.
type SessionClass int

const (
	// ClassAdmitted is a session already admitted to a pod: bound to it,
	// at least one turn dispatched, not paused. Its footprint is already
	// counted and finishing its trajectory is what frees capacity, so its
	// turns always dispatch.
	ClassAdmitted SessionClass = iota
	// ClassPaused gave its room to another session's turn: its next turn
	// must fit its own pod again, but it outranks never-admitted sessions.
	ClassPaused
	// ClassNew is not admitted to any pod: it never dispatched, or its pod
	// left the pool. Admitted only when a pod has room.
	ClassNew
)

func (c SessionClass) String() string {
	switch c {
	case ClassAdmitted:
		return "admitted"
	case ClassPaused:
		return "paused"
	default:
		return "new"
	}
}

// NeverReturns is the NextTurnIn of a session not expected back soon.
const NeverReturns = time.Duration(math.MaxInt64)

// SessionInfo is a read-only view of one idle session that can be paused.
type SessionInfo struct {
	ID string
	// Tokens is the session's size: the room it gives up if paused.
	Tokens         float64
	TurnCount      int64
	LastResponseAt time.Time
	IdleFor        time.Duration
	// NextTurnIn is the NextTurnPredictor's estimate for this session.
	NextTurnIn time.Duration
}

// Candidate is a read-only view of one queue head that fits a pod.
type Candidate struct {
	SessionID string
	Class     SessionClass
	// Tokens is the session's size once PreRequest charges this turn.
	Tokens float64
	// Wait is how long the head has been in the flow-control queue.
	Wait time.Duration
}

// NextTurnPredictor predicts when an idle session's next turn arrives.
type NextTurnPredictor interface {
	fwkplugin.Plugin
	// ObserveTurn records that a session's turn was dispatched. gap is the
	// idle time since its previous response, zero on its first turn. The
	// request lets a predictor read other producers' data, such as the
	// session-state-producer attribute.
	ObserveTurn(sessionID string, gap time.Duration, request *fwksched.InferenceRequest)
	// Forget drops a session the ledger evicted.
	Forget(sessionID string)
	// NextTurnIn predicts how long until an idle session's next turn
	// arrives, given it has been idle for idleFor.
	NextTurnIn(sessionID string, idleFor time.Duration) time.Duration
}

// AdmissionPolicy decides which waiting session is admitted and which idle
// sessions give up their room for it.
type AdmissionPolicy interface {
	fwkplugin.Plugin
	// Less orders the candidates that fit; the gate admits the first.
	// Heads past the starvation deadline are ordered by the gate first.
	Less(a, b *Candidate) bool
	// Victims returns, in pause order, the idle sessions on one pod that may
	// give up their room for a candidate of class forClass. Sessions left
	// out keep their room. The gate pauses a prefix until the room covers
	// the need. It must not modify idle.
	Victims(forClass SessionClass, idle []SessionInfo) []SessionInfo
}

// leasePredictor is the built-in NextTurnPredictor: a session is assumed to
// be back any moment for idleLeaseSeconds after its last response, and not
// soon after that.
type leasePredictor struct {
	lease time.Duration
}

func (p *leasePredictor) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "thunder-agent-lease-predictor", Name: "built-in"}
}

func (p *leasePredictor) ObserveTurn(string, time.Duration, *fwksched.InferenceRequest) {}

func (p *leasePredictor) Forget(string) {}

func (p *leasePredictor) NextTurnIn(_ string, idleFor time.Duration) time.Duration {
	if idleFor < p.lease {
		return 0
	}
	return NeverReturns
}

// defaultPolicy is the built-in AdmissionPolicy. Admission: class (admitted
// -> paused -> new) -> smallest footprint -> oldest. Victims: an admitted
// session's turn may take any idle session's room; a paused or new session
// only the room of sessions not expected back right away (NextTurnIn > 0).
// Those predicted to return latest go first, then the longest idle.
type defaultPolicy struct{}

func (defaultPolicy) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "thunder-agent-default-policy", Name: "built-in"}
}

func (defaultPolicy) Less(a, b *Candidate) bool {
	if a.Class != b.Class {
		return a.Class < b.Class
	}
	if a.Tokens != b.Tokens {
		return a.Tokens < b.Tokens
	}
	return a.Wait > b.Wait
}

func (defaultPolicy) Victims(forClass SessionClass, idle []SessionInfo) []SessionInfo {
	victims := make([]SessionInfo, 0, len(idle))
	for _, s := range idle {
		if forClass == ClassAdmitted || s.NextTurnIn > 0 {
			victims = append(victims, s)
		}
	}
	sort.Slice(victims, func(i, j int) bool {
		if victims[i].NextTurnIn != victims[j].NextTurnIn {
			return victims[i].NextTurnIn > victims[j].NextTurnIn
		}
		return victims[i].IdleFor > victims[j].IdleFor
	})
	return victims
}
