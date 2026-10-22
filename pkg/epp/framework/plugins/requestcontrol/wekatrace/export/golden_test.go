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

package export

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
)

var update = flag.Bool("update", false, "rewrite the testdata fixture and golden files")

const (
	fixtureDir = "testdata"
	fixture    = "testdata/records.jsonl"
	goldenDir  = "testdata/golden"
	sidecarOut = "sidecar.jsonl"
	// fixtureT0 is the first arrival in the fixture, unix ms.
	fixtureT0 = 1792771200123
)

// Mirrors of the aiperf WekaTrace v7 pydantic models, which reject unknown
// fields.
type wekaTrace struct {
	ID          string            `json:"id"`
	Models      []string          `json:"models"`
	BlockSize   int               `json:"block_size"`
	HashIDScope string            `json:"hash_id_scope"`
	Requests    []json.RawMessage `json:"requests"`
	Totals      map[string]any    `json:"totals"`
}

type wekaRequest struct {
	T         float64  `json:"t"`
	Type      string   `json:"type"`
	Model     string   `json:"model"`
	In        int      `json:"in"`
	Out       int      `json:"out"`
	HashIDs   []int    `json:"hash_ids"`
	APITime   float64  `json:"api_time"`
	TTFT      *float64 `json:"ttft"`
	ThinkTime float64  `json:"think_time"`
}

type wekaSubagent struct {
	T            float64           `json:"t"`
	Type         string            `json:"type"`
	AgentID      string            `json:"agent_id"`
	SubagentType string            `json:"subagent_type"`
	DurationMs   *int64            `json:"duration_ms"`
	TotalTokens  *int64            `json:"total_tokens"`
	ToolUseCount *int64            `json:"tool_use_count"`
	Status       string            `json:"status"`
	Models       []string          `json:"models"`
	Requests     []json.RawMessage `json:"requests"`
}

var (
	traceKeys    = []string{"id", "models", "block_size", "hash_id_scope", "requests"}
	requestKeys  = []string{"t", "type", "model", "in", "out", "hash_ids", "api_time", "think_time"}
	subagentKeys = []string{"t", "type", "agent_id", "subagent_type", "duration_ms", "total_tokens", "tool_use_count", "status", "models", "requests"}
)

func strictDecode(t *testing.T, data []byte, required []string, v any) {
	t.Helper()
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(data, &keys); err != nil {
		t.Fatalf("decode %s: %v", data, err)
	}
	for _, k := range required {
		if _, ok := keys[k]; !ok {
			t.Errorf("missing required key %q in %s", k, data)
		}
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		t.Fatalf("strict decode %s: %v", data, err)
	}
}

func checkRequest(t *testing.T, data []byte, seenHash map[int]bool) wekaRequest {
	t.Helper()
	var r wekaRequest
	strictDecode(t, data, requestKeys, &r)
	switch r.Type {
	case TypeStream:
	case TypeNormal:
		if r.TTFT != nil {
			t.Errorf("ttft on non-streaming request %s", data)
		}
	default:
		t.Errorf("request type %q, want s or n", r.Type)
	}
	if r.APITime >= 1e4 || r.T >= 1e4 || r.ThinkTime < 0 {
		t.Errorf("times not in seconds or negative: %s", data)
	}
	for _, h := range r.HashIDs {
		seenHash[h] = true
	}
	return r
}

// checkSchema decodes every trace file in dir against the aiperf mirrors and
// returns the decoded top-level requests and subagents for spot checks.
func checkSchema(t *testing.T, dir string) ([]wekaRequest, []wekaSubagent) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no trace files in %s: %v", dir, err)
	}
	var reqs []wekaRequest
	var subs []wekaSubagent
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var tr wekaTrace
		strictDecode(t, data, traceKeys, &tr)
		if tr.ID+".json" != filepath.Base(f) {
			t.Errorf("trace id %q does not match file %s", tr.ID, f)
		}
		if tr.BlockSize != 64 || tr.HashIDScope != "local" || len(tr.Models) == 0 || len(tr.Requests) == 0 {
			t.Errorf("trace header %s: block_size=%d hash_id_scope=%q models=%v requests=%d",
				f, tr.BlockSize, tr.HashIDScope, tr.Models, len(tr.Requests))
		}
		seenHash := map[int]bool{}
		for _, raw := range tr.Requests {
			var head struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(raw, &head); err != nil {
				t.Fatal(err)
			}
			if head.Type != TypeSubagent {
				reqs = append(reqs, checkRequest(t, raw, seenHash))
				continue
			}
			var sa wekaSubagent
			strictDecode(t, raw, subagentKeys, &sa)
			if len(sa.Models) == 0 || len(sa.Requests) == 0 {
				t.Errorf("subagent %s has no models or requests", sa.AgentID)
			}
			for _, inner := range sa.Requests {
				checkRequest(t, inner, seenHash)
			}
			subs = append(subs, sa)
		}
		for h := range seenHash {
			if h < 0 || h >= len(seenHash) {
				t.Errorf("trace %s: hash id %d is not dense over %d ids", tr.ID, h, len(seenHash))
			}
		}
	}
	return reqs, subs
}

func TestGolden(t *testing.T) {
	if *update {
		writeFixture(t)
	}
	res, err := Export(fixtureDir, Options{SkewTol: DefaultSkewTol, Seed: "golden"})
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if err := WriteTraces(out, res.Traces); err != nil {
		t.Fatal(err)
	}
	if err := WriteSidecar(filepath.Join(out, sidecarOut), res.Sidecar); err != nil {
		t.Fatal(err)
	}

	wantStats := Stats{
		Files: 1, Lines: 12, Malformed: 1, Duplicates: 1, Undecodable: 1, Estimate: 1,
		Retries: 1, Requests: 7, Traces: 3, Subagents: 1,
	}
	if diff := cmp.Diff(wantStats, res.Stats); diff != "" {
		t.Errorf("stats (-want +got):\n%s", diff)
	}

	reqs, subs := checkSchema(t, out)
	var first *wekaRequest
	for i := range reqs {
		if reqs[i].APITime == 5.396 {
			first = &reqs[i]
		}
	}
	if first == nil || first.Type != TypeStream || first.TTFT == nil || *first.TTFT != 5.018 || first.T != 0 {
		t.Errorf("record with api_ms=5396 not exported as t=0 api_time=5.396 ttft=5.018: %+v", first)
	}
	if len(subs) != 1 || subs[0].Status != StatusCancelled || !slices.Equal(subs[0].Models, []string{"small-model"}) {
		t.Errorf("subagents = %+v, want one cancelled small-model subagent", subs)
	}

	if *update {
		if err := os.RemoveAll(goldenDir); err != nil {
			t.Fatal(err)
		}
		if err := os.CopyFS(goldenDir, os.DirFS(out)); err != nil {
			t.Fatal(err)
		}
	}
	compareDirs(t, goldenDir, out)
}

func compareDirs(t *testing.T, want, got string) {
	t.Helper()
	names := func(dir string) []string {
		es, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, 0, len(es))
		for _, e := range es {
			out = append(out, e.Name())
		}
		return out
	}
	wantNames, gotNames := names(want), names(got)
	if diff := cmp.Diff(wantNames, gotNames); diff != "" {
		t.Fatalf("golden files (-want +got):\n%s\nrun with -update to regenerate", diff)
	}
	for _, n := range wantNames {
		w, err := os.ReadFile(filepath.Join(want, n))
		if err != nil {
			t.Fatal(err)
		}
		g, err := os.ReadFile(filepath.Join(got, n))
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(string(w), string(g)); diff != "" {
			t.Errorf("%s (-want +got):\n%s\nrun with -update to regenerate", n, diff)
		}
	}
}

// writeFixture regenerates testdata/records.jsonl: one session with a main
// chain across two replicas, a cancelled subagent, a retried 503, an
// undecodable delta, a rotated key id, a request without a session, an
// estimate-tokenizer session, a duplicate line, and a malformed line.
func writeFixture(t *testing.T) {
	t.Helper()
	g := &idGen{}
	m1 := g.fresh(8)
	m2 := g.extend(m1, 7, 3)
	m3 := g.extend(m2, 9, 2)
	s1 := g.fresh(4)
	s2 := g.extend(s1, 4, 2)
	const llama = "meta-llama/Llama-3.1-70B-Instruct"
	t0 := int64(fixtureT0)

	recs := mkAll(
		rspec{rid: "m1", sess: "S", gen: 1, arr: t0, api: 5396, ttft: 5018, prompt: 492, ids: m1, stream: true, model: llama, out: 250},
		rspec{rid: "solo", gen: 1, arr: t0 + 1000, api: 700, prompt: 150, ids: g.fresh(3), model: llama, out: 12},
		rspec{rid: "s1", sess: "S", gen: 1, arr: t0 + 6000, api: 2000, ids: s1, model: "small-model", out: 40},
		rspec{rid: "m2", sess: "S", gen: 1, arr: t0 + 7000, api: 3000, ttft: 800, prompt: 635, ids: m2, anchor: 7, stream: true, model: llama, out: 90},
		rspec{rid: "s2", sess: "S", gen: 1, arr: t0 + 8500, api: 1000, ids: s2, anchor: 4, model: "small-model", out: 3, cause: record.CauseClientDisconnect},
		rspec{rid: "und", sess: "S", gen: 1, arr: t0 + 9000, api: 1000, ids: g.extend(m2, 9, 1), anchor: 9, model: llama},
		rspec{rid: "m3", sess: "S", rep: "rep-b", gen: 7, arr: t0 + 12000, api: 2500, ttft: 2400, ids: m3, stream: true, model: llama, status: 503},
		rspec{rid: "m4", sess: "S", rep: "rep-b", gen: 7, arr: t0 + 13000, api: 2000, ttft: 300, ids: m3, anchor: 11, stream: true, model: llama, out: 77},
		rspec{rid: "k2", sess: "S", kid: "k2", rep: "rep-b", gen: 7, arr: t0 + 20000, api: 400, ids: g.fresh(2), model: llama, out: 5},
		rspec{rid: "est", sess: "E", gen: 1, arr: t0 + 21000, api: 400, ids: g.fresh(2), model: llama, backend: record.TokBackendEstimate},
	)
	recs[5].Anchor.H = record.HexID(1)
	cached, queue := 384, int64(12)
	recs[3].CachedTok, recs[3].QueueMs, recs[3].Ep, recs[3].Profile = &cached, &queue, "ep-1", "decode"
	recs[7].UsageSrc = record.UsageSrcEvents

	var buf bytes.Buffer
	for i, r := range recs {
		line, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
		if i == 0 {
			buf.Write(line)
			buf.WriteString("\n\n{not json\n")
		}
	}
	if err := os.MkdirAll(fixtureDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReadGzip(t *testing.T) {
	g := &idGen{}
	dir := t.TempDir()
	line, err := json.Marshal(mk(rspec{rid: "r1", sess: "s", ids: g.fresh(2)}))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(append(line, []byte("\ngarbage")...)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"a.jsonl.gz": buf.Bytes(),
		"b.jsonl":    line,
		"c.txt":      []byte("ignored"),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	recs, st, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(Stats{Files: 2, Lines: 3, Malformed: 1}, st); diff != "" {
		t.Errorf("stats (-want +got):\n%s", diff)
	}
	if len(recs) != 2 || recs[0].Rid != "r1" {
		t.Errorf("records = %+v", recs)
	}
}
