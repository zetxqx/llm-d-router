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
	"testing"

	"github.com/stretchr/testify/require"

	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/sessionstate"
)

// A bound session's pod gets 1.0, every other candidate 0.0.
func TestScorePinsBoundSession(t *testing.T) {
	a := newTestAgent(testConfig())
	podA, podB := schedEndpoint("pod-a"), schedEndpoint("pod-b")
	seed(a, "s1", "pod-a", 300)

	scores := a.Score(context.Background(), newRequest("s1", 400), []fwksched.Endpoint{podA, podB})
	require.Equal(t, 1.0, scores[podA])
	require.Equal(t, 0.0, scores[podB])
}

// The scorer reads the SessionState the session-state-producer publishes on
// the request.
func TestScoreReadsRequestAttribute(t *testing.T) {
	a := newThunderAgent("test", testConfig()) // no registry
	podA, podB := schedEndpoint("pod-a"), schedEndpoint("pod-b")
	req := newRequest("s1", 400)
	req.PutAttribute(sessionstate.SessionStateDataKey, sessionstate.SessionState{BoundEndpoint: podID("pod-b"), TurnsTaken: 1})

	scores := a.Score(context.Background(), req, []fwksched.Endpoint{podA, podB})
	require.Equal(t, 0.0, scores[podA])
	require.Equal(t, 1.0, scores[podB])
}

// A session Pick reserved onto a pod is pinned there before it is bound.
func TestScorePinsReservedSession(t *testing.T) {
	a := newTestAgent(testConfig())
	podA, podB := schedEndpoint("pod-a"), schedEndpoint("pod-b")
	seed(a, "s0", "pod-a", 900)
	primeFitView(a, "pod-a", "pod-b")

	q := makeQueue("s1", now(a), 2000)
	require.Equal(t, q, pick(t, a, q))
	scores := a.Score(context.Background(), newRequest("s1", 2000), []fwksched.Endpoint{podA, podB})
	require.Equal(t, 1.0, scores[podB])
}

// Unknown sessions and anonymous requests get no scores.
func TestScoreAbstains(t *testing.T) {
	a := newTestAgent(testConfig())
	podA := schedEndpoint("pod-a")

	require.Nil(t, a.Score(context.Background(), newRequest("unknown", 400), []fwksched.Endpoint{podA}))
	require.Nil(t, a.Score(context.Background(), newRequest("", 400), []fwksched.Endpoint{podA}))
}

// When the bound pod is not among the candidates (it left the pool or a
// filter removed it), the scorer abstains rather than zeroing everything.
func TestScoreAbstainsWhenBoundPodMissing(t *testing.T) {
	a := newTestAgent(testConfig())
	seed(a, "s1", "pod-a", 300)

	require.Nil(t, a.Score(context.Background(), newRequest("s1", 400), []fwksched.Endpoint{schedEndpoint("pod-b")}))
}

func TestScorerCategory(t *testing.T) {
	require.Equal(t, fwksched.Affinity, newTestAgent(testConfig()).Category())
}
