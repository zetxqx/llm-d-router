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
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

// Saturation never reports saturated, and logs each prefill pod once.
func TestSaturationLogsPrefillOnce(t *testing.T) {
	var lines []string
	ctx := log.IntoContext(context.Background(), funcr.New(func(_, args string) {
		lines = append(lines, args)
	}, funcr.Options{Verbosity: logutil.DEBUG}))
	a := newTestAgent(testConfig())
	prefill := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
		ID:     types.NamespacedName{Namespace: "default", Name: "pod-p"},
		Labels: map[string]string{bylabel.RoleLabel: bylabel.RolePrefill},
	}, nil)
	decode := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: types.NamespacedName{Namespace: "default", Name: "pod-d"}}, nil)

	for range 3 {
		require.Zero(t, a.Saturation(ctx, []fwkdl.Endpoint{prefill, decode}))
	}
	require.Equal(t, 1, strings.Count(strings.Join(lines, "\n"), "prefill/decode"))
}
