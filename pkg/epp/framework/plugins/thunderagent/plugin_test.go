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
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	eppdatalayer "github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionstate"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/requestheader/agentidentity"
	"github.com/llm-d/llm-d-router/test/utils"
)

var sessionStateDefaults = map[string]string{
	sessionstate.SessionStateDataKey.String(): sessionstate.SessionStateProducerType,
}

var sessionStateFactories = map[string]fwkplugin.FactoryFunc{
	sessionstate.SessionStateProducerType: sessionstate.Factory,
}

// Agent identity is a required input with no default producer, so
// configuration loading fails without an identity provider.
func TestAgentIdentityIsARequiredDependency(t *testing.T) {
	a := newTestAgent(testConfig())
	handle := fwkplugin.NewEppHandle(context.Background(), nil)
	handle.AddPlugin(a.TypedName().Name, a)

	err := eppdatalayer.CreateMissingDataProducers(context.Background(), sessionStateDefaults, sessionStateFactories, handle)
	require.ErrorIs(t, err, eppdatalayer.ErrNoDefaultProducer)
	require.ErrorContains(t, err, agentidentity.AgentIdentityKey.String())
}

// Session state is a required input: a session-state-producer is created when
// none is configured, and the plugin resolves its registry from the handle on
// first use.
func TestSessionStateProducerIsAutoCreated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handle := utils.NewTestHandle(ctx)
	p, err := Factory("thunder", nil, handle)
	require.NoError(t, err)
	a := p.(*ThunderAgent)
	handle.AddPlugin(a.TypedName().Name, a)
	identity, err := agentidentity.PluginFactory("agent-identity", nil, handle)
	require.NoError(t, err)
	handle.AddPlugin(identity.TypedName().Name, identity)

	require.Nil(t, a.sessions(), "no producer yet")
	require.NoError(t, eppdatalayer.CreateMissingDataProducers(ctx, sessionStateDefaults, sessionStateFactories, handle))
	producer, err := fwkplugin.PluginByType[*sessionstate.Producer](handle, sessionstate.SessionStateProducerType)
	require.NoError(t, err)
	require.Same(t, producer.Registry(), a.sessions())

	_, err = eppdatalayer.ValidateAndOrderDataDependencies(handle.GetAllPlugins())
	require.NoError(t, err)
}

func TestFactoryRejectsInvalidSweepInterval(t *testing.T) {
	_, err := Factory("thunder", json.NewDecoder(strings.NewReader(`{"evictionSweepSeconds": 0}`)), nil)
	require.ErrorContains(t, err, "evictionSweepSeconds")
}

// Session retention moved to the session-state-producer; the former TTL
// parameter is still accepted under strict decoding so existing
// configurations load.
func TestFactoryAcceptsDeprecatedEvictionTTL(t *testing.T) {
	_, err := Factory("thunder", fwkplugin.StrictDecoder(json.RawMessage(`{"evictionTtlSeconds": 3600}`)), nil)
	require.NoError(t, err)
}

// DumpState reports counts and per-endpoint totals but no session ids.
func TestDumpState(t *testing.T) {
	a := newTestAgent(testConfig())
	primeFitView(a, "pod-a")
	seed(a, "secret-session", "pod-a", 300)
	startTurn(a, "other", "pod-a", 800)
	seed(a, "paused", "pod-a", 200)
	pause(t, a, "paused")

	raw, err := a.DumpState()
	require.NoError(t, err)
	require.NotContains(t, string(raw), "secret-session")

	var got stateDump
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, stateDump{
		RunningSessions: 1,
		IdleSessions:    1,
		PausedSessions:  1,
		Endpoints: map[string]endpointDump{
			podID("pod-a"): {WorkingSetTokens: 500, CapacityTokens: 1000},
		},
	}, got)
}
