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

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

// Saturation always returns 0.0.
//
// Returning 0 is deliberate: the flow controller's saturation gate is a
// band-level head-of-line block, and while it reports saturated nothing
// dispatches, including the turns of sessions already admitted. Those turns
// are what complete trajectories and free capacity, so blocking them can
// only stall the pool. The per-session gate lives in Pick instead, where an
// admitted session's turn can be told apart from a new one.
//
// Prefill pods are logged once each, since prefill/decode disaggregation is
// not supported.
func (a *ThunderAgent) Saturation(ctx context.Context, endpoints []datalayer.Endpoint) float64 {
	var prefillPods []string
	a.mu.Lock()
	for _, endpoint := range endpoints {
		md := endpoint.GetMetadata()
		if md == nil {
			continue
		}
		id := md.ID.String()
		if role := md.Labels[bylabel.RoleLabel]; !a.prefillSeen[id] && (role == bylabel.RolePrefill || role == bylabel.RoleEncodePrefill) {
			a.prefillSeen[id] = true
			prefillPods = append(prefillPods, id)
		}
	}
	a.mu.Unlock()
	for _, id := range prefillPods {
		log.FromContext(ctx).V(logutil.DEFAULT).Info("thunder-agent does not support prefill/decode disaggregation: prefill pods enter its admission accounting", "pod", id)
	}
	return 0.0
}
