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
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
)

// Every move of an admitted session off its pod is logged at DEBUG.
func TestWarnings(t *testing.T) {
	var lines []string
	ctx := log.IntoContext(context.Background(), funcr.New(func(_, args string) {
		lines = append(lines, args)
	}, funcr.Options{Verbosity: logutil.DEBUG}))
	a := newTestAgent(testConfig())

	for _, pod := range []string{"pod-a", "pod-b", "pod-c"} {
		require.NoError(t, a.PreRequest(ctx, newRequest("s1", 400), schedulingResultFor(schedEndpoint(pod, 0, 0))))
	}
	require.Equal(t, 2, strings.Count(strings.Join(lines, "\n"), "moved off its pod"))
}
