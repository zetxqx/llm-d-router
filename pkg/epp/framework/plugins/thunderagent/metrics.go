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

	"github.com/prometheus/client_golang/prometheus"
	compbasemetrics "k8s.io/component-base/metrics"

	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
	eppmetrics "github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

// thunderMetrics holds the plugin's Prometheus collectors. Unregistered
// collectors still accept writes, so the hooks never need to nil-check.
type thunderMetrics struct {
	programs            *prometheus.GaugeVec
	podWorkingSetTokens *prometheus.GaugeVec
	podCapacityTokens   *prometheus.GaugeVec

	holds                *prometheus.CounterVec
	releases             *prometheus.CounterVec
	pauses               prometheus.Counter
	starvationPromotions prometheus.Counter
}

func newThunderMetrics() *thunderMetrics {
	return &thunderMetrics{
		programs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "thunder_agent_programs",
			Help:      metricsutil.HelpMsgWithStability("Tracked sessions by state.", compbasemetrics.ALPHA),
		}, []string{"state"}),
		podWorkingSetTokens: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "thunder_agent_pod_working_set_tokens",
			Help:      metricsutil.HelpMsgWithStability("Session KV working set per pod in tokens. Compare with the engine's KV utilization: a large working set over a low engine utilization is the KV-thrashing signature.", compbasemetrics.ALPHA),
		}, []string{"pod"}),
		podCapacityTokens: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "thunder_agent_pod_capacity_tokens",
			Help:      metricsutil.HelpMsgWithStability("Scraped KV token capacity per pod. A pod without this series reports no capacity and is not gated.", compbasemetrics.ALPHA),
		}, []string{"pod"}),
		holds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "thunder_agent_holds_total",
			Help:      metricsutil.HelpMsgWithStability("Dispatches whose head waited past the hold floor in the flow-control queue, by session class.", compbasemetrics.ALPHA),
		}, []string{"class"}),
		releases: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "thunder_agent_releases_total",
			Help:      metricsutil.HelpMsgWithStability("Dispatches picked by the thunder fairness policy, by session class.", compbasemetrics.ALPHA),
		}, []string{"class"}),
		pauses: prometheus.NewCounter(prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "thunder_agent_pauses_total",
			Help:      metricsutil.HelpMsgWithStability("Idle sessions paused to make room for another session's turn; their next turn must fit their pod again.", compbasemetrics.ALPHA),
		}),
		starvationPromotions: prometheus.NewCounter(prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "thunder_agent_starvation_promotions_total",
			Help:      metricsutil.HelpMsgWithStability("Queue heads force-admitted ahead of class, size, and fit by the starvation backstop.", compbasemetrics.ALPHA),
		}),
	}
}

// register registers the collectors, adopting an already-registered
// collector of the same name instead of failing, so a config reload that
// rebuilds the plugin against the same registry keeps working.
func (m *thunderMetrics) register(reg prometheus.Registerer) error {
	return errors.Join(
		registerOrReuse(reg, &m.programs),
		registerOrReuse(reg, &m.podWorkingSetTokens),
		registerOrReuse(reg, &m.podCapacityTokens),
		registerOrReuse(reg, &m.holds),
		registerOrReuse(reg, &m.releases),
		registerOrReuse(reg, &m.pauses),
		registerOrReuse(reg, &m.starvationPromotions),
	)
}

func registerOrReuse[C prometheus.Collector](reg prometheus.Registerer, target *C) error {
	err := reg.Register(*target)
	if err == nil {
		return nil
	}
	var already prometheus.AlreadyRegisteredError
	if errors.As(err, &already) {
		if existing, ok := already.ExistingCollector.(C); ok {
			*target = existing
			return nil
		}
	}
	return err
}

// publish reports a maintenance snapshot. The pod-labeled gauges are reset
// first so pods that left the ledger drop their series; publish runs at most
// once per maintenanceInterval, so the reset is cheap.
func (a *ThunderAgent) publish(snap *gaugeSnapshot) {
	if snap == nil {
		return
	}
	a.metrics.programs.WithLabelValues("running").Set(float64(snap.running))
	a.metrics.programs.WithLabelValues("idle").Set(float64(snap.idle))
	a.metrics.programs.WithLabelValues("paused").Set(float64(snap.paused))
	a.metrics.podWorkingSetTokens.Reset()
	a.metrics.podCapacityTokens.Reset()
	for pod, g := range snap.endpoints {
		a.metrics.podWorkingSetTokens.WithLabelValues(pod).Set(g.workingSet)
		a.metrics.podCapacityTokens.WithLabelValues(pod).Set(g.capacity)
	}
}
