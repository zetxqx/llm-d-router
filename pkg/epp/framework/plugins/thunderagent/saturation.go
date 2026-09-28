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
	"time"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

// Saturation refreshes the pod ledger from the live endpoint list, runs the
// ledger maintenance, and always returns 0.0.
//
// Returning 0 is deliberate: the flow controller's saturation gate is a
// band-level head-of-line block, and while it reports saturated nothing
// dispatches, including the turns of sessions already admitted. Those turns
// are what complete trajectories and free capacity, so blocking them can
// only stall the pool. The per-session gate lives in Pick instead, where an
// admitted session's turn can be told apart from a new one. This hook is
// used because it is the only flow-control callback that receives the
// endpoint list each dispatch cycle.
//
// Only pods that report their KV capacity enter the ledger. Without any, the
// plugin does nothing: Pick fails open and sessions are neither bound nor
// pinned.
func (a *ThunderAgent) Saturation(_ context.Context, endpoints []datalayer.Endpoint) float64 {
	now := time.Now()
	m := a.mgr

	m.mu.Lock()
	snap := m.maintainLocked(now)
	for _, endpoint := range endpoints {
		md := endpoint.GetMetadata()
		if md == nil {
			continue
		}
		if capacity := kvCapacity(endpoint.GetMetrics()); capacity > 0 {
			m.ensureEndpointLocked(md.ID.String(), capacity, now)
		}
	}
	// A pod the endpoint list has stopped reporting is gone: unbind its
	// sessions so their next turn re-enters as new, and drop the entry.
	for id, p := range m.endpoints {
		if now.Sub(p.updatedAt) <= endpointStaleAfter {
			continue
		}
		for sid, s := range p.sessions {
			s.endpoint = nil
			s.paused = false
			delete(p.sessions, sid)
		}
		delete(m.endpoints, id)
	}
	m.mu.Unlock()

	a.publish(snap)
	return 0.0
}

// kvCapacity returns a pod's KV cache capacity in tokens, block_size *
// num_gpu_blocks as scraped from the engine by the datalayer metrics
// extractor (built in for vLLM, SGLang and other engines), or 0 when it is
// not reported.
func kvCapacity(m *datalayer.Metrics) float64 {
	if m == nil || m.CacheBlockSize <= 0 || m.CacheNumBlocks <= 0 {
		return 0
	}
	return float64(m.CacheBlockSize) * float64(m.CacheNumBlocks)
}
