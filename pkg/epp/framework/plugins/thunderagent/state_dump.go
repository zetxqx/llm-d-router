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
	"encoding/json"
	"time"
)

// podDump is one pod's aggregate in the state dump.
type podDump struct {
	Programs int     `json:"programs"`
	Tokens   float64 `json:"tokens"`  // undecayed working set
	Decayed  float64 `json:"decayed"` // admission view
}

// dumpState is the sanitized snapshot returned by DumpState. Program IDs come
// from a user-controlled request header, so they are omitted; only bounded
// aggregates are reported.
type dumpState struct {
	TotalPrograms         int                `json:"totalPrograms"`
	TotalInflightTokens   int64              `json:"totalInflightTokens"`
	TotalCommittedTokens  int64              `json:"totalCommittedTokens"`
	PausedPrograms        int                `json:"pausedPrograms"`
	BytesPerToken         float64            `json:"bytesPerToken"`
	PausesTotal           int64              `json:"pausesTotal"`
	ResumesTotal          int64              `json:"resumesTotal"`
	OriginWaitsTotal      int64              `json:"originWaitsTotal"`
	UrgentPromotionsTotal int64              `json:"urgentPromotionsTotal"`
	Pods                  map[string]podDump `json:"pods"`
}

func (a *ThunderAgent) DumpState() (json.RawMessage, error) {
	now := time.Now()
	t := a.table
	t.mu.Lock()
	state := dumpState{
		BytesPerToken:         t.bytesPerToken,
		PausesTotal:           t.pausesTotal,
		ResumesTotal:          t.resumesTotal,
		OriginWaitsTotal:      t.originWaitsTotal,
		UrgentPromotionsTotal: t.urgentPromotionsTotal,
		Pods:                  make(map[string]podDump),
	}
	for _, st := range t.programs {
		state.TotalPrograms++
		if st.paused {
			state.PausedPrograms++
		}
		state.TotalInflightTokens += st.inflightTokens
		state.TotalCommittedTokens += st.committedTokens
	}
	for pod, load := range t.podLoads(now) {
		state.Pods[pod] = podDump{
			Programs: load.programs,
			Tokens:   load.undecayed,
			Decayed:  load.decayed,
		}
	}
	t.mu.Unlock()
	return json.Marshal(state)
}
