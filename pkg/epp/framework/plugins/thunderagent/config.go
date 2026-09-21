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
	"errors"
	"fmt"
	"strings"
)

// ResumePlacement values: where a paused program's next turn may be placed.
const (
	// ResumePlacementMostRoom resumes onto the origin pod when it fits, else
	// onto the pod with the most room (upstream's best-fit-decreasing
	// re-placement, which can move the program off its warm prefix cache).
	ResumePlacementMostRoom = "most-room"
	// ResumePlacementOriginOnly holds the turn until the origin pod has room.
	// The program moves only when the origin has left the pool or the
	// forced-admission backstop fires.
	ResumePlacementOriginOnly = "origin-only"
)

// Config holds the configuration for the ThunderAgent plugin.
type Config struct {
	// CapacityTokens is the per-endpoint KV cache capacity in tokens used when
	// the endpoint does not report cache_config_info (block_size *
	// num_gpu_blocks) through the datalayer.
	CapacityTokens int64 `json:"capacityTokens"`
	// RequireRealCapacity, when true, treats an endpoint without scraped
	// capacity as having no room for new programs instead of falling back to
	// CapacityTokens.
	RequireRealCapacity bool `json:"requireRealCapacity"`
	// UtilThreshold is the admission fit ceiling: a new program is admitted
	// only onto a pod whose working set plus the program's projected footprint
	// stays within this fraction of the pod's capacity. Turns of programs
	// already admitted bypass this ceiling; their footprint is already
	// counted.
	UtilThreshold float64 `json:"utilThreshold"`
	// ActingHalfLifeSeconds is the half-life applied to the committed tokens
	// of a program with no request in flight, in the admission view only.
	// Upstream ThunderAgent uses a fixed 2^-t with t in seconds, i.e. 1. 0
	// disables decay.
	ActingHalfLifeSeconds float64 `json:"actingHalfLifeSeconds"`
	// BufferTokensPerProgram is added to a program's footprint in the working
	// set and in the admission fit check, reserving growth room for the
	// context a session accumulates across its turns.
	BufferTokensPerProgram int64 `json:"bufferTokensPerProgram"`
	// PauseSweepSeconds is how often the pause sweep runs per pod (upstream
	// scheduler_interval). While a pod's undecayed working set exceeds the
	// fit ceiling, the sweep pauses its idle programs smallest first and, when
	// none are left, marks every in-flight program to pause at the end of its
	// turn. 0 sweeps on every dispatch cycle.
	PauseSweepSeconds float64 `json:"pauseSweepSeconds"`
	// KVUsageCorrection subtracts, per pod, the difference between the
	// estimated footprint of in-flight programs and the engine's reported KV
	// usage (upstream shared_tokens). Upstream never activates this path, so
	// it is off by default; it needs real scraped capacity and metrics.
	KVUsageCorrection bool `json:"kvUsageCorrection"`
	// ResumePlacement decides where a paused program's next turn may go:
	// "most-room" (default) prefers the origin pod and falls back to the pod
	// with the most room; "origin-only" holds the turn until the origin pod
	// has room, unless the origin left the pool or HeadWaitStarvationMs
	// fires. New programs always go to the pod with the most room.
	ResumePlacement string `json:"resumePlacement"`
	// UrgentWaitMs is the urgent tier: a paused or new program whose head has
	// waited at least this long is ordered ahead of every non-urgent paused or
	// new program (oldest first) and, under origin-only placement, may take
	// any pod with room instead of waiting for its origin. It still needs a
	// pod with room (unlike HeadWaitStarvationMs). Set it to the TTFT SLO
	// minus the cost of one re-prefill. 0 disables the tier. Not upstream
	// behaviour (proposal Part 1 aging plus Part 3 Option B).
	UrgentWaitMs float64 `json:"urgentWaitMs"`
	// UrgentMove lets an urgent paused program leave its origin pod for the
	// pod with the most room under origin-only placement. Off, the urgent
	// tier only reorders (age-only): the program still waits for its origin.
	UrgentMove bool `json:"urgentMove"`
	// UrgentReserveOrigin reserves a pod for the urgent paused programs
	// waiting on it: while such a program does not fit, no new program and no
	// non-urgent paused program is admitted onto that pod, so freed room
	// accumulates for the oldest waiter instead of going to smaller newcomers.
	// Turns of programs already running on the pod are unaffected.
	UrgentReserveOrigin bool `json:"urgentReserveOrigin"`
	// HeadWaitStarvationMs promotes any queue whose head has waited at least
	// this long ahead of class, size and fit. This is the forced-admission
	// backstop (upstream _wait_for_resume timeout, 1800 s). 0 disables it.
	HeadWaitStarvationMs float64 `json:"headWaitStarvationMs"`
	// EvictionTTLSeconds is how long a program with no in-flight request and
	// no activity is kept before its state is dropped.
	EvictionTTLSeconds float64 `json:"evictionTtlSeconds"`
	// EvictionSweepSeconds is how often expired program state is swept.
	EvictionSweepSeconds float64 `json:"evictionSweepSeconds"`
	// SessionFinalHeader is the request header that marks the session's last
	// turn. The program's state is released when that turn completes instead
	// of waiting for the eviction TTL.
	SessionFinalHeader string `json:"sessionFinalHeader"`
	// ParentSessionHeader carries the parent session ID of a subagent. It is
	// recorded for observability only; accounting is not shared.
	ParentSessionHeader string `json:"parentSessionHeader"`
	// ProfileName selects which profile's picked endpoint a program is
	// attributed to. When empty, the primary (decode) profile is used.
	ProfileName string `json:"profileName"`
}

func defaultConfig() Config {
	return Config{
		CapacityTokens:         4194304,
		UtilThreshold:          1.0,
		ActingHalfLifeSeconds:  1,
		BufferTokensPerProgram: 100,
		PauseSweepSeconds:      5,
		ResumePlacement:        ResumePlacementMostRoom,
		HeadWaitStarvationMs:   1800000,
		EvictionTTLSeconds:     3600,
		EvictionSweepSeconds:   300,
		SessionFinalHeader:     "x-session-final",
		ParentSessionHeader:    "x-parent-session-id",
	}
}

func (c Config) validate() error {
	if c.CapacityTokens <= 0 {
		return fmt.Errorf("capacityTokens must be > 0, got %d", c.CapacityTokens)
	}
	if c.UtilThreshold <= 0 || c.UtilThreshold > 1 {
		return fmt.Errorf("utilThreshold must be in (0, 1], got %v", c.UtilThreshold)
	}
	if c.ActingHalfLifeSeconds < 0 {
		return fmt.Errorf("actingHalfLifeSeconds must be >= 0, got %v", c.ActingHalfLifeSeconds)
	}
	if c.BufferTokensPerProgram < 0 {
		return fmt.Errorf("bufferTokensPerProgram must be >= 0, got %d", c.BufferTokensPerProgram)
	}
	if c.PauseSweepSeconds < 0 {
		return fmt.Errorf("pauseSweepSeconds must be >= 0, got %v", c.PauseSweepSeconds)
	}
	if c.ResumePlacement != ResumePlacementMostRoom && c.ResumePlacement != ResumePlacementOriginOnly {
		return fmt.Errorf("resumePlacement must be %q or %q, got %q", ResumePlacementMostRoom, ResumePlacementOriginOnly, c.ResumePlacement)
	}
	if c.UrgentWaitMs < 0 {
		return fmt.Errorf("urgentWaitMs must be >= 0, got %v", c.UrgentWaitMs)
	}
	if (c.UrgentMove || c.UrgentReserveOrigin) && c.UrgentWaitMs <= 0 {
		return errors.New("urgentMove and urgentReserveOrigin need urgentWaitMs > 0")
	}
	if c.UrgentWaitMs > 0 && c.HeadWaitStarvationMs > 0 && c.UrgentWaitMs >= c.HeadWaitStarvationMs {
		return fmt.Errorf("urgentWaitMs (%v) must be below headWaitStarvationMs (%v), or the urgent tier never applies", c.UrgentWaitMs, c.HeadWaitStarvationMs)
	}
	if c.HeadWaitStarvationMs < 0 {
		return fmt.Errorf("headWaitStarvationMs must be >= 0, got %v", c.HeadWaitStarvationMs)
	}
	if c.EvictionTTLSeconds <= 0 {
		return fmt.Errorf("evictionTtlSeconds must be > 0, got %v", c.EvictionTTLSeconds)
	}
	if c.EvictionSweepSeconds <= 0 {
		return fmt.Errorf("evictionSweepSeconds must be > 0, got %v", c.EvictionSweepSeconds)
	}
	if c.EvictionTTLSeconds*1000 <= c.HeadWaitStarvationMs {
		return fmt.Errorf("evictionTtlSeconds (%v s) must exceed headWaitStarvationMs (%v ms), or a held program is evicted mid-wait and re-enters as new",
			c.EvictionTTLSeconds, c.HeadWaitStarvationMs)
	}
	if strings.TrimSpace(c.SessionFinalHeader) == "" {
		return errors.New("sessionFinalHeader must not be empty")
	}
	return nil
}
