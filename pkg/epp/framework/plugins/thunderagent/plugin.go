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

// Package thunderagent gates admission of agentic sessions on the KV cache
// footprint they hold on each pod, based on ThunderAgent (arXiv 2602.13692).
//
// A session (an agent trajectory identified by the session id the
// agent-identity plugin publishes) is bound to one pod. Its binding, turn
// counts and KV token footprint are tracked by the session-state-producer in a
// sessionstate.SessionStateRegistry; this plugin reads them. The engine's own
// KV utilization cannot serve this purpose: a session waiting on a tool call
// still owns its context in the prefix cache, but those blocks sit on the free
// list and are reported as unused, so on an agentic workload the reported
// utilization stays low while the cache is in fact full.
//
// The plugin keeps only its own policy state: which sessions it paused to make
// room, and the short reservation between Pick and PreRequest (policy.go).
package thunderagent

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/log"

	fwkfc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionstate"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/requestheader/agentidentity"
)

// ThunderAgentPluginType is the plugin type registered with the framework.
const ThunderAgentPluginType = "thunder-agent"

var (
	_ fwkplugin.ConsumerPlugin = &ThunderAgent{}
	_ fwkplugin.StateDumper    = &ThunderAgent{}
	_ fwksched.Scorer          = &ThunderAgent{}
	_ fwkfc.SaturationDetector = &ThunderAgent{}
	_ fwkfc.FairnessPolicy     = &ThunderAgent{}
)

// ThunderAgent is a single named instance shared by every hookup, so the
// scorer and the admission gate read the same policy state.
type ThunderAgent struct {
	typedName fwkplugin.TypedName

	capacityTokens       float64
	utilThreshold        float64
	headWaitStarvationMs float64
	idleLease            time.Duration

	clock clock.PassiveClock
	// handle resolves the session-state-producer's registry on first use: the
	// producer may be auto-created after this plugin.
	handle         fwkplugin.HandlePlugins
	ctx            context.Context
	registry       atomic.Pointer[sessionstate.SessionStateRegistry]
	lookupFailures atomic.Int64

	// mu guards local and prefillSeen.
	mu    sync.Mutex
	local map[string]*localSession
	// prefillSeen records prefill pods already logged as unsupported.
	prefillSeen map[string]bool

	metrics *thunderMetrics
}

// Factory builds a ThunderAgent from raw plugin parameters, registers its
// metrics and starts the sweep of its policy state for the plugin's lifetime.
func Factory(name string, rawParameters *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	cfg := defaultConfig()
	if rawParameters != nil {
		if err := rawParameters.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' plugin: %w", ThunderAgentPluginType, err)
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid parameters of the '%s' plugin: %w", ThunderAgentPluginType, err)
	}

	a := newThunderAgent(name, cfg)
	if handle != nil {
		a.handle = handle
		a.ctx = handle.Context()
		if cfg.EvictionTTLSeconds != 0 {
			log.FromContext(a.ctx).Info("thunder-agent ignores evictionTtlSeconds; set it on the session-state-producer", "plugin", name)
		}
		if reg := handle.Metrics(); reg != nil {
			if err := a.metrics.register(reg); err != nil {
				return nil, fmt.Errorf("failed to register metrics of the '%s' plugin: %w", ThunderAgentPluginType, err)
			}
		}
		sweepInterval := time.Duration(cfg.EvictionSweepSeconds * float64(time.Second))
		go a.runSweep(handle.Context(), sweepInterval)
	}
	return a, nil
}

// runSweep drops expired policy state every interval until ctx is cancelled.
func (a *ThunderAgent) runSweep(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.sweep()
		}
	}
}

func newThunderAgent(name string, cfg Config) *ThunderAgent {
	a := &ThunderAgent{
		typedName:            fwkplugin.TypedName{Type: ThunderAgentPluginType, Name: name},
		capacityTokens:       float64(cfg.CapacityTokens),
		utilThreshold:        cfg.UtilThreshold,
		headWaitStarvationMs: cfg.HeadWaitStarvationMs,
		idleLease:            time.Duration(cfg.IdleLeaseSeconds * float64(time.Second)),
		clock:                clock.RealClock{},
		ctx:                  context.Background(),
		local:                make(map[string]*localSession),
		prefillSeen:          make(map[string]bool),
	}
	a.metrics = newThunderMetrics(a)
	return a
}

func (a *ThunderAgent) TypedName() fwkplugin.TypedName {
	return a.typedName
}

// Consumes declares agent identity and session state as required inputs. Agent
// identity has no default producer, so configuration loading fails when no
// identity provider is enabled; a session-state-producer is created when none
// is configured.
func (a *ThunderAgent) Consumes() fwkplugin.DataDependencies {
	return fwkplugin.DataDependencies{
		Required: map[fwkplugin.DataKey]any{
			agentidentity.AgentIdentityKey: "",
			sessionstate.SessionStateDataKey.WithNonEmptyProducerName(sessionstate.SessionStateProducerType): sessionstate.SessionState{},
		},
	}
}

// sessions returns the session-state registry, or nil when it cannot be
// resolved yet.
func (a *ThunderAgent) sessions() *sessionstate.SessionStateRegistry {
	if r := a.registry.Load(); r != nil {
		return r
	}
	if a.handle == nil {
		return nil
	}
	producer, err := sessionstate.LookupProducer(a.handle)
	if err != nil {
		// Logged on the first failure only: Pick runs every dispatch cycle.
		if a.lookupFailures.Add(1) == 1 {
			log.FromContext(a.ctx).Error(err, "thunder-agent cannot read session state; the admission gate fails open")
		}
		return nil
	}
	if ttl := producer.EvictionTTL(); ttl > 0 && a.headWaitStarvationMs > 0 && float64(ttl.Milliseconds()) <= a.headWaitStarvationMs {
		log.FromContext(a.ctx).Info("session-state-producer evictionTtlSeconds does not exceed thunder-agent headWaitStarvationMs: a held session can be evicted mid-wait and re-enter as new",
			"evictionTtl", ttl, "headWaitStarvationMs", a.headWaitStarvationMs)
	}
	r := producer.Registry()
	a.registry.Store(r)
	return r
}

// sessionID returns the session identifier published by the agent-identity
// plugin, or "" for requests carrying none. The fairness ID is not used: an
// explicit fairness header often carries a tenant rather than a session.
// Anonymous traffic is not tracked.
func sessionID(request *fwksched.InferenceRequest) string {
	if request == nil {
		return ""
	}
	id, _ := fwksched.ReadRequestAttribute[string](request, agentidentity.AgentIdentityKey)
	return id
}

// sessionState returns the request's session state, read from the request
// attribute when the session-state-producer published one, else from the
// registry.
func (a *ThunderAgent) sessionState(request *fwksched.InferenceRequest, id string) (sessionstate.SessionState, bool) {
	if state, ok := sessionstate.ReadSessionState(request); ok {
		return state, true
	}
	if reg := a.sessions(); reg != nil {
		return reg.State(id)
	}
	return sessionstate.SessionState{}, false
}

// stateDump is the snapshot returned by DumpState. Session ids come from
// client headers, so they are omitted; sessions are only counted.
type stateDump struct {
	RunningSessions int                     `json:"runningSessions"`
	IdleSessions    int                     `json:"idleSessions"`
	PausedSessions  int                     `json:"pausedSessions"`
	Endpoints       map[string]endpointDump `json:"endpoints"`
}

type endpointDump struct {
	WorkingSetTokens float64 `json:"workingSetTokens"`
	CapacityTokens   float64 `json:"capacityTokens"`
}

// DumpState reports session counts and each endpoint's working set and
// capacity, the same values as the metrics.
func (a *ThunderAgent) DumpState() (json.RawMessage, error) {
	snap := a.snapshot()
	dump := stateDump{
		RunningSessions: snap.running,
		IdleSessions:    snap.idle,
		PausedSessions:  snap.paused,
		Endpoints:       make(map[string]endpointDump, len(snap.endpoints)),
	}
	for id, e := range snap.endpoints {
		dump.Endpoints[id] = endpointDump{WorkingSetTokens: e.workingSet, CapacityTokens: e.capacity}
	}
	return json.Marshal(dump)
}
