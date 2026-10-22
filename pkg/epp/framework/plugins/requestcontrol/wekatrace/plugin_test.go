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

package wekatrace

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2s"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
)

const testSession = "11111111-2222-3333-4444-555555555555"

var requestSeq atomic.Int64

func TestConfigResolve(t *testing.T) {
	base := func() Config {
		return Config{Mode: ModeDogfood, KeySecretPath: "/k"}
	}
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{name: "dogfood defaults", mutate: func(*Config) {}},
		{name: "missing mode", mutate: func(c *Config) { c.Mode = "" }, wantErr: "mode must be"},
		{name: "unknown mode", mutate: func(c *Config) { c.Mode = "prod" }, wantErr: "mode must be"},
		{name: "customer without consent", mutate: func(c *Config) { c.Mode = ModeCustomer }, wantErr: "consent"},
		{name: "customer with stdout", mutate: func(c *Config) { c.Mode, c.Consent, c.Sink.Stdout = ModeCustomer, true, true }, wantErr: "stdout"},
		{name: "customer ok", mutate: func(c *Config) { c.Mode, c.Consent = ModeCustomer, true }},
		{name: "missing key path", mutate: func(c *Config) { c.KeySecretPath = "" }, wantErr: "keySecretPath"},
		{name: "rate out of range", mutate: func(c *Config) { c.SampleRate = ptr(1.5) }, wantErr: "sampleRate"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			tc.mutate(&c)
			r, err := c.settings()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, defaultSampleRate, r.sampleRate)
			if c.Mode == ModeCustomer {
				assert.Equal(t, int64(customerArrivalResolutionMs), r.arrivalResolutionMs)
			}
		})
	}
}

type harness struct {
	t      *testing.T
	dir    string
	rec    *Recorder
	cancel context.CancelFunc
	now    time.Time
}

func newHarness(t *testing.T, mutate func(*settings)) *harness {
	t.Helper()
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(keyPath, []byte("test-key-material-0123456789"), 0o600))
	cfg, err := Config{Mode: ModeDogfood, KeySecretPath: keyPath, KeyID: "k1", SampleRate: ptr(1.0),
		Sink: SinkConfig{Path: filepath.Join(dir, "out")}}.settings()
	require.NoError(t, err)
	if mutate != nil {
		mutate(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := newRecorder(ctx, "test", cfg, func() string { return "test" })
	require.NotNil(t, r.worker)
	return &harness{t: t, dir: cfg.sinkPath, rec: r, cancel: cancel, now: time.UnixMilli(1_800_000_000_000)}
}

func tokens(n int, offset uint32) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = offset + uint32(i)
	}
	return out
}

// turn drives one request through PreRequest and the end-of-stream hook.
func (h *harness) turn(sessionID string, toks []uint32, stream bool) {
	h.t.Helper()
	req := &fwksched.InferenceRequest{
		RequestID:   fmt.Sprintf("uuid-%d", requestSeq.Add(1)),
		TargetModel: "m",
		Headers:     map[string]string{},
		Body: &fwkrh.InferenceRequestBody{
			Stream:           stream,
			TokenizedRequest: fwkrh.NewTokenizedRequest([][]uint32{toks}),
		},
	}
	if sessionID != "" {
		req.Headers["x-claude-code-session-id"] = sessionID
	}
	require.NoError(h.t, h.rec.PreRequest(context.Background(), req, &fwksched.SchedulingResult{}))
	h.now = h.now.Add(2 * time.Second)
	h.rec.ResponseBody(context.Background(), req, &requestcontrol.Response{
		RequestID:        req.RequestID,
		EndOfStream:      true,
		Usage:            fwkrh.Usage{PromptTokens: len(toks), CompletionTokens: 7},
		TerminationCause: requestcontrol.TerminationCauseNatural,
		Timing: &requestcontrol.ResponseTiming{
			RequestReceived: h.now,
			FirstByte:       h.now.Add(300 * time.Millisecond),
			Completed:       h.now.Add(1500 * time.Millisecond),
		},
	}, nil)
}

// finish stops the worker and returns every record from closed files, with
// the raw file contents for leak checks.
func (h *harness) finish() ([]record.Record, map[string][]record.Record, string) {
	h.t.Helper()
	time.Sleep(50 * time.Millisecond)
	h.cancel()
	var files []string
	require.Eventually(h.t, func() bool {
		partial, _ := filepath.Glob(filepath.Join(h.dir, "*"+partialSuffix))
		files, _ = filepath.Glob(filepath.Join(h.dir, "*"+finalSuffix))
		return len(partial) == 0 && len(files) > 0
	}, 5*time.Second, 10*time.Millisecond)
	var all []record.Record
	perFile := map[string][]record.Record{}
	var raw strings.Builder
	for _, f := range files {
		b, err := os.ReadFile(f)
		require.NoError(h.t, err)
		raw.Write(b)
		sc := bufio.NewScanner(strings.NewReader(string(b)))
		sc.Buffer(make([]byte, 1<<20), 16<<20)
		for sc.Scan() {
			var r record.Record
			require.NoError(h.t, json.Unmarshal(sc.Bytes(), &r))
			require.NoError(h.t, r.Validate())
			all = append(all, r)
			perFile[f] = append(perFile[f], r)
		}
	}
	return all, perFile, raw.String()
}

func TestRecorderDeltaAcrossTurns(t *testing.T) {
	h := newHarness(t, nil)
	turn1 := tokens(200, 0)
	turn2 := append(append([]uint32{}, turn1...), tokens(100, 10_000)...)
	turn3 := append(append([]uint32{}, turn2...), tokens(64, 20_000)...)
	h.turn(testSession, turn1, true)
	h.turn(testSession, turn2, true)
	h.turn(testSession, turn3, false)
	recs, _, raw := h.finish()
	require.Len(t, recs, 3)

	assert.True(t, recs[0].Kf)
	assert.Equal(t, 4, recs[0].Blocks) // ceil(200/64)
	require.NotNil(t, recs[1].Anchor)
	assert.Equal(t, 3, recs[1].Anchor.N) // 200/64 full blocks shared
	assert.Equal(t, 5, recs[1].Blocks)   // ceil(300/64)
	require.NotNil(t, recs[2].Anchor)
	assert.Equal(t, 4, recs[2].Anchor.N) // 300/64 full blocks shared

	first, err := record.DecodeIDs(recs[0].New)
	require.NoError(t, err)
	assert.Equal(t, record.HexID(first[2]), recs[1].Anchor.H)

	for _, r := range recs {
		require.NotNil(t, r.Sess)
		assert.Equal(t, *recs[0].Sess, *r.Sess)
		assert.Equal(t, int64(1500), r.APIMs)
		assert.Equal(t, 7, r.OutTok)
		assert.Equal(t, record.UsageSrcServer, r.UsageSrc)
		assert.Equal(t, "k1", r.Kid)
		assert.True(t, strings.HasPrefix(r.Model, "model-"))
	}
	require.NotNil(t, recs[0].TTFTMs)
	assert.Equal(t, int64(300), *recs[0].TTFTMs)
	assert.Nil(t, recs[2].TTFTMs, "ttft only for streaming requests")

	assert.NotContains(t, raw, testSession, "raw session id must not be written")
	assert.NotContains(t, raw, "uuid-", "raw request id must not be written")
}

func TestRecorderSessionsAreUnlinkable(t *testing.T) {
	h := newHarness(t, nil)
	same := tokens(256, 0)
	h.turn("session-a", same, true)
	h.turn("session-b", same, true)
	recs, _, _ := h.finish()
	require.Len(t, recs, 2)
	a, err := record.DecodeIDs(recs[0].New)
	require.NoError(t, err)
	b, err := record.DecodeIDs(recs[1].New)
	require.NoError(t, err)
	for i := range a {
		assert.NotEqual(t, a[i], b[i], "equal content in different sessions must map to different ids")
	}
}

func TestRecorderFileRotationKeepsFilesSelfDecodable(t *testing.T) {
	h := newHarness(t, func(c *settings) { c.fileBytes = 1 })
	turn1 := tokens(128, 0)
	turn2 := append(append([]uint32{}, turn1...), tokens(64, 5_000)...)
	h.turn(testSession, turn1, true)
	h.turn(testSession, turn2, true)
	_, perFile, _ := h.finish()
	require.Len(t, perFile, 2)
	for f, recs := range perFile {
		require.Len(t, recs, 1, f)
		assert.True(t, recs[0].Kf, "first record of a session in each file must be a keyframe")
	}
}

func TestRecorderCustomerCoarsensArrival(t *testing.T) {
	h := newHarness(t, func(c *settings) { c.arrivalResolutionMs = customerArrivalResolutionMs })
	h.now = h.now.Add(1234 * time.Millisecond)
	h.turn(testSession, tokens(64, 0), true)
	recs, _, _ := h.finish()
	require.Len(t, recs, 1)
	assert.Zero(t, recs[0].ArrMs%customerArrivalResolutionMs)
}

func TestSamplingIsDeterministicAcrossReplicas(t *testing.T) {
	cfg := settings{sampleRate: 0.3}
	key := blake2s.Sum256([]byte("shared-key"))
	a := &worker{cfg: cfg, key: key}
	b := &worker{cfg: cfg, key: key}
	sampled := 0
	for i := range 4000 {
		sid := "session-" + time.Duration(i).String()
		assert.Equal(t, a.sampled(sid, "r"), b.sampled(sid, "r"))
		if a.sampled(sid, "r") {
			sampled++
		}
	}
	assert.InDelta(t, 0.3, float64(sampled)/4000, 0.03)
}

func TestRecorderDisabledWithoutKey(t *testing.T) {
	cfg, err := Config{Mode: ModeDogfood, KeySecretPath: filepath.Join(t.TempDir(), "missing")}.settings()
	require.NoError(t, err)
	r := newRecorder(context.Background(), "test", cfg, func() string { return "test" })
	assert.Nil(t, r.worker)
	req := &fwksched.InferenceRequest{RequestID: "r", Headers: map[string]string{}}
	assert.NoError(t, r.PreRequest(context.Background(), req, nil))
	r.ResponseBody(context.Background(), req, &requestcontrol.Response{EndOfStream: true}, nil)
}

func TestHookPanicIsContained(t *testing.T) {
	h := newHarness(t, nil)
	defer h.cancel()
	assert.NotPanics(t, func() {
		defer h.rec.recoverHook(context.Background(), "test")
		panic("boom")
	})
	assert.Equal(t, int64(1), h.rec.stats.panics.Load())
}
