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
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFactory(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		p, err := Factory("test", nil, nil)
		require.NoError(t, err)
		a, ok := p.(*ThunderAgent)
		require.True(t, ok)
		// Upstream ThunderAgent tr-decay defaults: fit against full capacity,
		// 2^-t decay in seconds, 5 s scheduler tick, 1800 s forced resume,
		// no shared_tokens correction (dead code upstream).
		assert.Equal(t, float64(4194304), a.capacityTokens)
		assert.Equal(t, 1.0, a.utilThreshold)
		assert.Equal(t, time.Second, a.table.actingHalfLife)
		assert.Equal(t, 5*time.Second, a.pauseSweep)
		assert.Equal(t, 1800000.0, a.headWaitStarvationMs)
		assert.False(t, a.kvUsageCorrection)
		assert.Equal(t, 100.0, a.bufferTokensPerProgram)
		assert.Equal(t, "x-session-final", a.sessionFinalHeader)
		assert.False(t, a.resumeOriginOnly, "default resume placement is most-room")
		assert.Equal(t, 0.0, a.urgentWaitMs, "urgent tier off by default")
		assert.False(t, a.urgentMove)
		assert.False(t, a.urgentReserveOrigin)
	})

	t.Run("urgent tier", func(t *testing.T) {
		params := json.NewDecoder(bytes.NewBufferString(`{"urgentWaitMs": 15000}`))
		p, err := Factory("test", params, nil)
		require.NoError(t, err)
		assert.Equal(t, 15000.0, p.(*ThunderAgent).urgentWaitMs)
	})

	t.Run("origin-only resume placement", func(t *testing.T) {
		params := json.NewDecoder(bytes.NewBufferString(`{"resumePlacement": "origin-only"}`))
		p, err := Factory("test", params, nil)
		require.NoError(t, err)
		assert.True(t, p.(*ThunderAgent).resumeOriginOnly)
	})

	t.Run("header names are normalized", func(t *testing.T) {
		params := json.NewDecoder(bytes.NewBufferString(`{"sessionFinalHeader": " X-Session-Final "}`))
		p, err := Factory("test", params, nil)
		require.NoError(t, err)
		assert.Equal(t, "x-session-final", p.(*ThunderAgent).sessionFinalHeader)
	})

	invalid := map[string]string{
		"capacity":         `{"capacityTokens": -1}`,
		"half life":        `{"actingHalfLifeSeconds": -1}`,
		"util threshold":   `{"utilThreshold": 1.5}`,
		"buffer tokens":    `{"bufferTokensPerProgram": -1}`,
		"starvation":       `{"headWaitStarvationMs": -1}`,
		"ttl":              `{"evictionTtlSeconds": 0}`,
		"sweep":            `{"evictionSweepSeconds": 0}`,
		"pause sweep":      `{"pauseSweepSeconds": -1}`,
		"ttl below hold":   `{"evictionTtlSeconds": 60}`, // a held program would be evicted mid-wait
		"final header":     `{"sessionFinalHeader": " "}`,
		"placement":        `{"resumePlacement": "nearest"}`,
		"urgent":           `{"urgentWaitMs": -1}`,
		"urgent above":     `{"urgentWaitMs": 1800000}`, // not below headWaitStarvationMs: the tier would never apply
		"move w/o tier":    `{"urgentMove": true}`,
		"reserve w/o tier": `{"urgentReserveOrigin": true}`,
	}
	for name, raw := range invalid {
		t.Run("invalid "+name, func(t *testing.T) {
			_, err := Factory("test", json.NewDecoder(bytes.NewBufferString(raw)), nil)
			assert.Error(t, err)
		})
	}
}

func TestDumpState(t *testing.T) {
	a := newTestAgent(testConfig())
	endpoints := newTestEndpoints("pod1")

	req := newRequest("program-a", 400)
	require.NoError(t, a.PreRequest(context.Background(), req, schedulingResultFor(endpoints[0])))

	raw, err := a.DumpState()
	require.NoError(t, err)
	var state dumpState
	require.NoError(t, json.Unmarshal(raw, &state))
	assert.Equal(t, 1, state.TotalPrograms)
	assert.Equal(t, int64(100), state.TotalInflightTokens)
	assert.Equal(t, 0, state.PausedPrograms)
	assert.Equal(t, int64(0), state.PausesTotal)
	require.Contains(t, state.Pods, endpoints[0].GetMetadata().ID.String())
	assert.Equal(t, 1, state.Pods[endpoints[0].GetMetadata().ID.String()].Programs)
	assert.Equal(t, 100.0, state.Pods[endpoints[0].GetMetadata().ID.String()].Tokens)
}
