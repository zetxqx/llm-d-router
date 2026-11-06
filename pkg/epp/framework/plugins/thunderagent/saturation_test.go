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
	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

// When a pod stops being reported, its sessions re-enter as new.
func TestPodVanishUnbinds(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(t, a, "s1", "pod-a", 300)
	a.mgr.mu.Lock()
	a.mgr.endpoints["default/pod-a"].updatedAt = time.Now().Add(-2 * endpointStaleAfter)
	a.mgr.mu.Unlock()
	primeFitView(a, dlEndpoint("pod-b"))

	s, ok := sessionOf(a, "s1")
	require.True(t, ok)
	require.Nil(t, s.endpoint)
}

// Under prefill/decode disaggregation only the pods that decode enter the
// ledger: a session's KV stays on its decode pod between turns.
func TestOnlyDecodePodsEnterLedger(t *testing.T) {
	a := newTestAgent(testConfig())
	withRole := func(role string) fwkdl.Endpoint {
		return fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
			ID:     types.NamespacedName{Namespace: "default", Name: role},
			Labels: map[string]string{bylabel.RoleLabel: role},
		}, &fwkdl.Metrics{})
	}
	primeFitView(a,
		withRole(bylabel.RoleDecode), withRole(bylabel.RolePrefillDecode), withRole(bylabel.RoleEncodePrefillDecode),
		withRole(bylabel.RolePrefill), withRole(bylabel.RoleEncodePrefill), withRole(bylabel.RoleEncode),
		dlEndpoint("no-role"))

	a.mgr.mu.Lock()
	defer a.mgr.mu.Unlock()
	ids := make([]string, 0, len(a.mgr.endpoints))
	for id := range a.mgr.endpoints {
		ids = append(ids, id)
	}
	require.ElementsMatch(t, []string{"default/decode", "default/prefill-decode", "default/encode-prefill-decode", "default/no-role"}, ids)
}
