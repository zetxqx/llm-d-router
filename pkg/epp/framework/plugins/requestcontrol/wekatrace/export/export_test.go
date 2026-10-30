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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
)

// idGen yields distinct block ids. Tests build prompts that share a prefix
// and then diverge, matching the chained-hash property of real ids.
type idGen struct{ next uint64 }

func (g *idGen) fresh(n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		g.next++
		out[i] = g.next * 0x9e3779b97f4a7c15
	}
	return out
}

// extend returns prefix[:k] followed by n fresh ids.
func (g *idGen) extend(prefix []uint64, k, n int) []uint64 {
	return append(append([]uint64{}, prefix[:k]...), g.fresh(n)...)
}

// rspec describes a test record. Zero values pick defaults in mk.
type rspec struct {
	rid, sess, kid, rep string // empty sess means no session
	gen                 uint64
	arr, api, ttft      int64 // ttft 0 means absent
	prompt              int   // 0 means 64 per block
	ids                 []uint64
	anchor              int // 0 means keyframe
	stream              bool
	cause               string
	status              int
	model, backend      string
	out                 int
}

func mk(s rspec) record.Record {
	or := func(v, def string) string {
		if v == "" {
			return def
		}
		return v
	}
	r := record.Record{
		V: record.Version, Kid: or(s.kid, "k1"), Rid: s.rid, Rep: or(s.rep, "rep-a"), Gen: s.gen,
		ArrMs: s.arr, Model: or(s.model, "m1"), Stream: s.stream, TokBackend: or(s.backend, "vllm"),
		PromptTok: s.prompt, Blocks: len(s.ids), New: record.EncodeIDs(s.ids[s.anchor:]), Kf: s.anchor == 0,
		OutTok: s.out, UsageSrc: record.UsageSrcServer, APIMs: s.api, Status: s.status,
		Cause: or(s.cause, record.CauseNatural),
	}
	if s.sess != "" {
		r.Sess = &s.sess
	}
	if r.PromptTok == 0 {
		r.PromptTok = record.BlockSizeTokens * len(s.ids)
	}
	if r.Status == 0 {
		r.Status = 200
	}
	if s.anchor > 0 {
		r.Anchor = &record.Anchor{N: s.anchor, H: record.HexID(s.ids[s.anchor-1])}
	}
	if s.ttft > 0 {
		r.TTFTMs = &s.ttft
	}
	return r
}

func mkAll(specs ...rspec) []record.Record {
	out := make([]record.Record, len(specs))
	for i, s := range specs {
		out[i] = mk(s)
	}
	return out
}

func build(t *testing.T, recs []record.Record, opts Options) *Result {
	t.Helper()
	if opts.Seed == "" {
		opts.Seed = "test"
	}
	res, err := Build(recs, opts)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestDecode(t *testing.T) {
	g := &idGen{}
	a := g.fresh(4)
	d1 := g.extend(a, 4, 2)
	d2 := g.extend(a, 2, 3)
	d3 := g.extend(d1, 6, 2)
	missing := mk(rspec{rid: "missing", arr: 40, ids: g.extend(a, 3, 1), anchor: 3})
	missing.Anchor.H = record.HexID(12345)
	recs := append(mkAll(
		rspec{rid: "kf", arr: 0, ids: a},
		// d3 anchors on d1, which arrives later.
		rspec{rid: "d3", arr: 5, ids: d3, anchor: 6},
		rspec{rid: "d1", arr: 10, ids: d1, anchor: 4},
		rspec{rid: "d2", arr: 20, ids: d2, anchor: 2},
		rspec{rid: "other-gen", gen: 2, arr: 30, ids: d1, anchor: 4},
		rspec{rid: "other-rep", rep: "rep-b", arr: 30, ids: d1, anchor: 4},
	), missing)

	ptrs := make([]*record.Record, len(recs))
	for i := range recs {
		ptrs[i] = &recs[i]
	}
	ds, undec := decode(ptrs)
	got := map[string][]uint64{}
	for _, d := range ds {
		got[d.rec.Rid] = d.ids
	}
	if diff := cmp.Diff(map[string][]uint64{"kf": a, "d1": d1, "d2": d2, "d3": d3}, got); diff != "" {
		t.Errorf("decoded ids (-want +got):\n%s", diff)
	}
	if undec != 3 {
		t.Errorf("undecodable = %d, want 3", undec)
	}
}

// chains returns the rids of each exported chain: the main chain first, then
// each subagent in entry order.
func chains(t *testing.T, recs []record.Record, tol time.Duration) [][]string {
	t.Helper()
	var out [][]string
	pos := map[string]int{}
	for _, r := range build(t, recs, Options{SkewTol: tol}).Sidecar {
		i, ok := pos[r.AgentID]
		if !ok {
			i = len(out)
			pos[r.AgentID] = i
			out = append(out, nil)
		}
		out[i] = append(out[i], r.Rid)
	}
	return out
}

func TestLineage(t *testing.T) {
	g := &idGen{}
	lin1 := g.fresh(4)
	lin2 := g.extend(lin1, 4, 2)
	lin3 := g.extend(lin2, 6, 2)

	tail1 := g.fresh(4)
	tail2 := g.extend(tail1, 3, 3)
	short2 := g.extend(tail1, 2, 3)

	m1 := g.fresh(6)
	m2 := g.extend(m1, 6, 2)
	m3 := g.extend(m2, 8, 2)
	s1 := g.fresh(3)
	s2 := g.extend(s1, 3, 2)

	p1 := g.fresh(6)
	p2 := g.extend(p1, 6, 2)
	shared := g.fresh(5)
	a2 := g.extend(shared, 5, 2)
	b2 := g.extend(shared, 5, 2)

	tests := []struct {
		name string
		recs []record.Record
		want [][]string
	}{
		{
			name: "linear main chain",
			recs: mkAll(
				rspec{rid: "r1", sess: "s", arr: 0, api: 1000, ids: lin1},
				rspec{rid: "r2", sess: "s", arr: 2000, api: 1000, ids: lin2, anchor: 4},
				rspec{rid: "r3", sess: "s", arr: 4000, api: 1000, ids: lin3, anchor: 6},
			),
			want: [][]string{{"r1", "r2", "r3"}},
		},
		{
			name: "last full block of the parent rewritten",
			recs: mkAll(
				rspec{rid: "r1", sess: "s", arr: 0, api: 1000, ids: tail1},
				rspec{rid: "r2", sess: "s", arr: 2000, api: 1000, ids: tail2, anchor: 3},
			),
			want: [][]string{{"r1", "r2"}},
		},
		{
			name: "shared prefix too short",
			recs: mkAll(
				rspec{rid: "r1", sess: "s", arr: 0, api: 1000, ids: tail1},
				rspec{rid: "r2", sess: "s", arr: 2000, api: 1000, ids: short2, anchor: 2},
			),
			want: [][]string{{"r1"}, {"r2"}},
		},
		{
			name: "one subagent",
			recs: mkAll(
				rspec{rid: "r1", sess: "s", arr: 0, api: 1000, ids: m1},
				rspec{rid: "s1", sess: "s", arr: 500, api: 800, ids: s1},
				rspec{rid: "s2", sess: "s", arr: 1500, api: 500, ids: s2, anchor: 3},
				rspec{rid: "r2", sess: "s", arr: 2000, api: 1000, ids: m2, anchor: 6},
				rspec{rid: "r3", sess: "s", arr: 4000, api: 1000, ids: m3, anchor: 8},
			),
			want: [][]string{{"r1", "r2", "r3"}, {"s1", "s2"}},
		},
		{
			// a1 and b1 send the same prompt; each parent takes one child,
			// so they form two chains. a2 prefers b1 for its later end time.
			name: "two parallel subagents with identical first prompts",
			recs: mkAll(
				rspec{rid: "r1", sess: "s", arr: 0, api: 500, ids: p1},
				rspec{rid: "a1", sess: "s", arr: 1000, api: 1000, ids: shared},
				rspec{rid: "b1", sess: "s", arr: 1000, api: 1200, ids: shared},
				rspec{rid: "a2", sess: "s", arr: 3000, api: 1000, ids: a2, anchor: 5},
				rspec{rid: "b2", sess: "s", arr: 3100, api: 1000, ids: b2, anchor: 5},
				rspec{rid: "r2", sess: "s", arr: 5000, api: 1000, ids: p2, anchor: 6},
			),
			want: [][]string{{"r1", "r2"}, {"a1", "b2"}, {"b1", "a2"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, chains(t, tc.recs, DefaultSkewTol)); diff != "" {
				t.Errorf("chains (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLineageSkew(t *testing.T) {
	g := &idGen{}
	p := g.fresh(4)
	c := g.extend(p, 4, 2)
	linked := [][]string{{"p", "c"}}
	split := [][]string{{"p"}, {"c"}}

	tests := []struct {
		name  string
		gapMs int64 // child arrival minus parent end
		tol   time.Duration
		want  [][]string
	}{
		{name: "child 50ms after parent end", gapMs: 50, tol: DefaultSkewTol, want: linked},
		{name: "child 50ms before parent end", gapMs: -50, tol: DefaultSkewTol, want: linked},
		{name: "child 300ms before parent end", gapMs: -300, tol: DefaultSkewTol, want: split},
		{name: "child 50ms before parent end without tolerance", gapMs: -50, want: split},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recs := mkAll(
				rspec{rid: "p", sess: "s", arr: 0, api: 1000, ids: p},
				rspec{rid: "c", sess: "s", arr: 1000 + tc.gapMs, api: 1000, ids: c, anchor: 4},
			)
			if diff := cmp.Diff(tc.want, chains(t, recs, tc.tol)); diff != "" {
				t.Errorf("chains (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRetryDedup(t *testing.T) {
	g := &idGen{}
	a := g.fresh(4)

	type row struct {
		Rid   string
		Retry bool
	}
	tests := []struct {
		name        string
		recs        []record.Record
		want        []row
		wantRetries int
	}{
		{
			name: "repeated failures keep the last attempt",
			recs: mkAll(
				rspec{rid: "r1", sess: "s", arr: 0, api: 100, ids: a, status: 429},
				rspec{rid: "r2", sess: "s", arr: 1000, api: 100, ids: a, status: 500},
				rspec{rid: "r3", sess: "s", arr: 2000, api: 100, ids: a},
			),
			want:        []row{{"r3", true}},
			wantRetries: 2,
		},
		{
			name: "client disconnect retried at the window edge",
			recs: mkAll(
				rspec{rid: "r1", sess: "s", arr: 0, api: 100, ids: a, cause: record.CauseClientDisconnect},
				rspec{rid: "r2", sess: "s", arr: 2000, api: 100, ids: a},
			),
			want:        []row{{"r2", true}},
			wantRetries: 1,
		},
		{
			name: "successful identical request kept",
			recs: mkAll(
				rspec{rid: "r1", sess: "s", arr: 0, api: 100, ids: a},
				rspec{rid: "r2", sess: "s", arr: 1000, api: 100, ids: a},
			),
			want: []row{{"r1", false}, {"r2", false}},
		},
		{
			name: "outside window",
			recs: mkAll(
				rspec{rid: "r1", sess: "s", arr: 0, api: 100, ids: a, status: 429},
				rspec{rid: "r2", sess: "s", arr: 2001, api: 100, ids: a},
			),
			want: []row{{"r1", false}, {"r2", false}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := build(t, tc.recs, Options{SkewTol: DefaultSkewTol})
			var got []row
			for _, r := range res.Sidecar {
				got = append(got, row{r.Rid, r.Retry})
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("sidecar (-want +got):\n%s", diff)
			}
			if res.Stats.Retries != tc.wantRetries || res.Traces[0].Totals.LLMD.Retries != tc.wantRetries {
				t.Errorf("retries: stats %d, totals %d, want %d",
					res.Stats.Retries, res.Traces[0].Totals.LLMD.Retries, tc.wantRetries)
			}
		})
	}
}

func TestThinkTime(t *testing.T) {
	g := &idGen{}
	r1 := g.fresh(4)
	r2 := g.extend(r1, 4, 2)
	r3 := g.extend(r2, 6, 2)
	recs := mkAll(
		// r2 arrives 100ms before r1 ends, within the skew tolerance.
		rspec{rid: "r1", sess: "s", arr: 0, api: 1000, ids: r1},
		rspec{rid: "r2", sess: "s", arr: 900, api: 600, ids: r2, anchor: 4},
		rspec{rid: "r3", sess: "s", arr: 3000, api: 500, ids: r3, anchor: 6},
	)
	entries := build(t, recs, Options{SkewTol: DefaultSkewTol}).Traces[0].Requests
	got := make([]float64, 0, len(entries))
	for _, e := range entries {
		if e.Request == nil {
			t.Fatalf("unexpected subagent entry %+v", e.Subagent)
		}
		got = append(got, e.Request.ThinkTime)
	}
	if diff := cmp.Diff([]float64{0, 0, 1.5}, got); diff != "" {
		t.Errorf("think_time (-want +got):\n%s", diff)
	}
}

func TestBuildGrouping(t *testing.T) {
	g := &idGen{}
	a, b, c, d := g.fresh(3), g.fresh(3), g.fresh(3), g.fresh(3)
	recs := mkAll(
		rspec{rid: "x1", sess: "s", arr: 0, api: 100, ids: a},
		rspec{rid: "x2", sess: "s", kid: "k2", arr: 10, api: 100, ids: b},
		rspec{rid: "n1", arr: 20, api: 100, ids: c},
		rspec{rid: "n2", arr: 30, api: 100, ids: c},
		rspec{rid: "e1", sess: "e", arr: 40, api: 100, ids: d, backend: record.TokBackendEstimate},
		rspec{rid: "x1", sess: "s", arr: 50, api: 100, ids: a},
	)
	bad := mk(rspec{rid: "bad", sess: "s", arr: 60, ids: a})
	bad.Blocks++
	recs = append(recs, bad)

	tests := []struct {
		name       string
		opts       Options
		wantTraces []string
		wantStats  Stats
	}{
		{
			name:       "estimate refused",
			wantTraces: []string{"x1", "x2", "n1", "n2"},
			wantStats:  Stats{Invalid: 1, Duplicates: 1, Estimate: 1},
		},
		{
			name:       "estimate allowed",
			opts:       Options{AllowEstimate: true},
			wantTraces: []string{"x1", "x2", "n1", "n2", "e1"},
			wantStats:  Stats{Invalid: 1, Duplicates: 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := build(t, recs, tc.opts)
			var got []string
			for i, r := range res.Sidecar {
				if i >= len(res.Traces) || r.Trace != res.Traces[i].ID {
					t.Fatalf("sidecar row %d (%s) is not the only request of trace %d", i, r.Rid, i)
				}
				got = append(got, r.Rid)
			}
			if diff := cmp.Diff(tc.wantTraces, got); diff != "" {
				t.Errorf("traces (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantStats, res.Stats); diff != "" {
				t.Errorf("stats (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTraceIDs(t *testing.T) {
	g := &idGen{}
	recs := mkAll(rspec{rid: "r1", sess: "s", arr: 0, api: 100, ids: g.fresh(2)})
	id := func(seed string) string {
		res, err := Build(recs, Options{Seed: seed})
		if err != nil {
			t.Fatal(err)
		}
		return res.Traces[0].ID
	}
	if a1, a2, b := id("a"), id("a"), id("b"); a1 != a2 || a1 == b {
		t.Error("seeded trace ids are not a deterministic function of the seed")
	}
	if r1, r2 := id(""), id(""); r1 == r2 || len(r1) != 2*traceIDBytes {
		t.Errorf("random trace ids %q, %q: want distinct %d-char hex", r1, r2, 2*traceIDBytes)
	}
}
