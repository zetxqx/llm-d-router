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

package epp

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/export"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
	"github.com/llm-d/llm-d-router/pkg/epp/metadata"
	"github.com/llm-d/llm-d-router/test/integration"
)

const wekaTraceConfigTemplate = `apiVersion: llm-d.ai/v1
kind: EndpointPickerConfig
plugins:
  - type: openai-parser
  - type: mock-metrics-source
  - type: weka-trace-recorder
    parameters:
      mode: dogfood
      keySecretPath: %s
      keyID: it
      sampleRate: 1.0
      sink:
        path: %s
schedulingProfiles:
  - name: default
requestHandler:
  parsers:
  - pluginRef: openai-parser
dataLayer:
  injectDefaults: false
  sources:
  - pluginRef: mock-metrics-source
`

// TestWekaTraceRecorder drives three turns of one session through the ext_proc
// server and checks the records the recorder writes.
func TestWekaTraceRecorder(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(keyPath, []byte("integration-test-key"), 0o600))
	sinkDir := filepath.Join(dir, "records")
	cfg := fmt.Sprintf(wekaTraceConfigTemplate, keyPath, sinkDir)

	h := NewTestHarness(ctx, t, WithStandardMode(), WithConfigText(cfg)).WithBaseResources()
	pods := []PodState{P(0, 0, 0.1, modelMyModelTarget)}
	h.WithPods(pods).WaitForSync(len(pods), modelMyModel)
	h.WaitForReadyPodsMetric(len(pods))

	const session = "it-session-0001"
	prompt := strings.Repeat("You are a coding agent. ", 40)
	for turn := range 3 {
		prompt += strings.Repeat(fmt.Sprintf("tool result %d. ", turn), 20)
		body, err := json.Marshal(map[string]any{
			"model": "modelName", "prompt": prompt, "max_tokens": 100, "stream": true,
		})
		require.NoError(t, err)
		requests := integration.ReqRaw(map[string]string{
			reqcommon.RequestIDHeaderKey: fmt.Sprintf("it-req-%d", turn),
			metadata.ModelNameRewriteKey: "modelName",
			"x-claude-code-session-id":   session,
		}, string(body))
		requests = append(requests, ReqResponseOnly(
			map[string]string{"content-type": "text/event-stream", "status": "200"},
			`data: {"choices":[{"delta":{"content":"ok"},"index":0,"finish_reason":null}],"id":"1","created":1,"model":"modelName","object":"text_completion"}
`,
			`data: {"choices":[{"delta":{},"index":0,"finish_reason":"stop"}],"id":"1","created":1,"model":"modelName","object":"text_completion","usage":{"completion_tokens":11,"prompt_tokens":321,"total_tokens":332}}
`,
			`data: [DONE]
`,
		)...)
		// One ext_proc stream carries one HTTP request, so each turn opens its own.
		client, err := extProcPb.NewExternalProcessorClient(h.grpcConn).Process(ctx)
		require.NoError(t, err)
		_, err = integration.StreamedRequest(t, client, requests, 6)
		require.NoError(t, client.CloseSend())
		require.NoError(t, err)
	}

	var recs []record.Record
	var raw strings.Builder
	require.Eventually(t, func() bool {
		files, _ := filepath.Glob(filepath.Join(sinkDir, "*.jsonl*"))
		recs = nil
		raw.Reset()
		for _, f := range files {
			b, err := os.ReadFile(f)
			if err != nil {
				return false
			}
			raw.Write(b)
			for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
				if line == "" {
					continue
				}
				var r record.Record
				if json.Unmarshal([]byte(line), &r) == nil {
					recs = append(recs, r)
				}
			}
		}
		return len(recs) == 3
	}, 10*time.Second, 50*time.Millisecond)

	for i, r := range recs {
		require.NoError(t, r.Validate(), "record %d", i)
		require.NotNil(t, r.Sess)
		assert.Equal(t, *recs[0].Sess, *r.Sess)
		assert.Equal(t, 11, r.OutTok)
		assert.Equal(t, record.UsageSrcServer, r.UsageSrc)
		assert.True(t, r.Stream)
		assert.Equal(t, "natural", r.Cause)
		assert.Positive(t, r.Blocks)
		assert.NotNil(t, r.TTFTMs)
	}
	assert.True(t, recs[0].Kf)
	for _, r := range recs[1:] {
		require.NotNil(t, r.Anchor, "later turns must be delta-encoded")
		assert.Positive(t, r.Anchor.N)
	}
	assert.NotContains(t, raw.String(), session)
	assert.NotContains(t, raw.String(), "it-req-")

	// The offline exporter turns the records into one WekaTrace session with a
	// linear main chain whose block ids share the growing prefix.
	res, err := export.Build(recs, export.Options{AllowEstimate: true, SkewTol: 250 * time.Millisecond, Seed: "it"})
	require.NoError(t, err)
	require.Len(t, res.Traces, 1)
	trace := res.Traces[0]
	assert.Equal(t, record.BlockSizeTokens, trace.BlockSize)
	assert.Equal(t, "local", trace.HashIDScope)
	require.Len(t, trace.Requests, 3)
	var prev []int
	for i, e := range trace.Requests {
		require.NotNil(t, e.Request, "entry %d must be a main-chain request", i)
		r := e.Request
		assert.Equal(t, "s", r.Type)
		assert.Equal(t, 321, r.In)
		assert.Equal(t, 11, r.Out)
		assert.GreaterOrEqual(t, r.ThinkTime, 0.0)
		if prev != nil {
			require.Greater(t, len(r.HashIDs), len(prev))
			assert.Equal(t, prev[:len(prev)-1], r.HashIDs[:len(prev)-1], "turn %d must extend the previous prefix", i)
		}
		prev = r.HashIDs
	}
}
