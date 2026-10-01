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
)

func TestSessionClassString(t *testing.T) {
	require.Equal(t, "admitted", ClassAdmitted.String())
	require.Equal(t, "paused", ClassPaused.String())
	require.Equal(t, "new", ClassNew.String())
}

// The lease predictor expects a session back any moment inside its lease,
// and not soon after it.
func TestLeasePredictor(t *testing.T) {
	p := &leasePredictor{lease: 30 * time.Second}
	require.Equal(t, time.Duration(0), p.NextTurnIn("s1", 29*time.Second))
	require.Equal(t, NeverReturns, p.NextTurnIn("s1", 30*time.Second))
}

// The default order: class, then the smaller footprint, then the older head.
func TestDefaultPolicyLess(t *testing.T) {
	p := defaultPolicy{}
	require.True(t, p.Less(&Candidate{Class: ClassAdmitted, Tokens: 900}, &Candidate{Class: ClassPaused, Tokens: 100}))
	require.True(t, p.Less(&Candidate{Class: ClassNew, Tokens: 100}, &Candidate{Class: ClassNew, Tokens: 200}))
	require.True(t, p.Less(&Candidate{Class: ClassNew, Tokens: 100, Wait: 2 * time.Second},
		&Candidate{Class: ClassNew, Tokens: 100, Wait: time.Second}))
}

// A paused or new session may take only the room of sessions not expected
// back right away; an admitted turn any idle session's. Those predicted back
// latest go first, then the longest idle. The input is left untouched.
func TestDefaultPolicyVictims(t *testing.T) {
	idle := []SessionInfo{
		{ID: "fresh", IdleFor: 5 * time.Second, NextTurnIn: 0},
		{ID: "old", IdleFor: 3 * time.Minute, NextTurnIn: NeverReturns},
		{ID: "mid", IdleFor: 2 * time.Minute, NextTurnIn: NeverReturns},
		{ID: "soon", IdleFor: time.Minute, NextTurnIn: time.Second},
	}
	before := append([]SessionInfo(nil), idle...)
	ids := func(v []SessionInfo) []string {
		out := make([]string, len(v))
		for i, s := range v {
			out[i] = s.ID
		}
		return out
	}
	p := defaultPolicy{}
	require.Equal(t, []string{"old", "mid", "soon"}, ids(p.Victims(ClassNew, idle)))
	require.Equal(t, []string{"old", "mid", "soon"}, ids(p.Victims(ClassPaused, idle)))
	require.Equal(t, []string{"old", "mid", "soon", "fresh"}, ids(p.Victims(ClassAdmitted, idle)))
	require.Equal(t, before, idle)
}
