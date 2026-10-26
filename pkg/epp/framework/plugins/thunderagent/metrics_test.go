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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	eppmetrics "github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

// The gauges are computed at scrape time, so they follow registry eviction
// with no request in between.
func TestLedgerGaugesWithoutTraffic(t *testing.T) {
	a := newTestAgent(testConfig())
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, a.metrics.register(reg))
	registryOf(a).UpsertEndpoint(podID("pod-a"), func() int64 { return 1600 })
	seed(a, "s1", "pod-a", 300)

	require.Equal(t, map[string]float64{
		`endpoint_capacity_tokens{endpoint="default/pod-a"}`:    1600,
		`endpoint_working_set_tokens{endpoint="default/pod-a"}`: 300,
		`sessions{state="idle"}`:                                1,
		`sessions{state="paused"}`:                              0,
		`sessions{state="running"}`:                             0,
	}, gather(t, reg))

	registryOf(a).EvictIdle(now(a).Add(2*time.Hour), time.Hour)
	registryOf(a).RemoveEndpoint(podID("pod-a"))

	require.Equal(t, map[string]float64{
		`sessions{state="idle"}`:    0,
		`sessions{state="paused"}`:  0,
		`sessions{state="running"}`: 0,
	}, gather(t, reg))
}

// A second instance's ledger collector is rejected, and the first instance
// keeps reporting.
func TestSecondInstanceRegistrationFails(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	first := newTestAgent(testConfig())
	require.NoError(t, first.metrics.register(reg))
	primeFitView(first, "pod-a")
	seed(first, "s1", "pod-a", 300)

	second := newTestAgent(testConfig())
	require.Error(t, second.metrics.register(reg))
	require.Equal(t, float64(1), gather(t, reg)[`sessions{state="idle"}`])
}

// gather scrapes reg and returns the thunder_agent gauges keyed by name
// (without the common prefix) and labels.
func gather(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	prefix := eppmetrics.LLMDRouterEndpointPickerSubsystem + "_thunder_agent_"
	out := map[string]float64{}
	for _, f := range families {
		name, ok := strings.CutPrefix(f.GetName(), prefix)
		if !ok || f.GetType().String() != "GAUGE" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := make([]string, 0, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				labels = append(labels, l.GetName()+`="`+l.GetValue()+`"`)
			}
			sort.Strings(labels)
			out[name+"{"+strings.Join(labels, ",")+"}"] = m.GetGauge().GetValue()
		}
	}
	return out
}
