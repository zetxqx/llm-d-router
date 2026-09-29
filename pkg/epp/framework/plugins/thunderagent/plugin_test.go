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
	"time"

	"github.com/stretchr/testify/require"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

func factory(t *testing.T, params string, handle fwkplugin.Handle) (*ThunderAgent, error) {
	t.Helper()
	p, err := Factory("thunder", json.NewDecoder(strings.NewReader(params)), handle)
	if err != nil {
		return nil, err
	}
	return p.(*ThunderAgent), nil
}

// Without references the plugin uses the built-in lease predictor and policy.
func TestFactoryDefaults(t *testing.T) {
	a, err := factory(t, `{"idleLeaseSeconds": 10}`, nil)
	require.NoError(t, err)
	require.Equal(t, &leasePredictor{lease: 10 * time.Second}, a.predictor)
	require.Equal(t, defaultPolicy{}, a.policy)
}

// References resolve to the named plugins, and the ledger's evictions reach
// the referenced predictor.
func TestFactoryResolvesPolicyReferences(t *testing.T) {
	handle := fwkplugin.NewEppHandle(context.Background(), nil)
	pred, pol := newFakePredictor(), &fakePolicy{}
	handle.AddPlugin("pred", pred)
	handle.AddPlugin("pol", pol)

	a, err := factory(t, `{"nextTurnPredictor": "pred", "admissionPolicy": "pol"}`, handle)
	require.NoError(t, err)
	require.Same(t, pred, a.predictor)
	require.Same(t, pol, a.policy)

	a.mgr.mu.Lock()
	a.mgr.bindLocked("s1", a.mgr.ensureEndpointLocked("default/ep-a", 1000, t0))
	a.mgr.removeLocked("s1")
	a.mgr.mu.Unlock()
	require.Equal(t, []string{"s1"}, pred.forgotten)
}

// A reference to a missing plugin, to a plugin of the wrong kind, or with no
// handle to resolve it fails.
func TestFactoryRejectsBadReferences(t *testing.T) {
	handle := fwkplugin.NewEppHandle(context.Background(), nil)
	handle.AddPlugin("pol", &fakePolicy{})

	_, err := factory(t, `{"nextTurnPredictor": "missing"}`, handle)
	require.Error(t, err)
	_, err = factory(t, `{"nextTurnPredictor": "pol"}`, handle)
	require.Error(t, err, "a policy is not a predictor")
	_, err = factory(t, `{"admissionPolicy": "pol"}`, nil)
	require.Error(t, err, "no handle")
}
