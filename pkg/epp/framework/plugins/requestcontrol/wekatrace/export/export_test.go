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
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
)

func ptrs(recs []record.Record) []*record.Record {
	out := make([]*record.Record, len(recs))
	for i := range recs {
		out[i] = &recs[i]
	}
	return out
}

func TestDecodeAll(t *testing.T) {
	g := &idGen{}
	a := g.fresh(4)
	d1 := g.extend(a, 4, 2)
	d2 := g.extend(d1, 6, 1)
	d3 := g.extend(a, 2, 3)
	d4 := g.extend(d1, 6, 2)

	missing := mk(rspec{rid: "missing", sess: "s", arr: 40, ids: g.extend(a, 3, 1), anchor: 3})
	missing.Anchor.H = record.HexID(12345)

	tests := []struct {
		name      string
		recs      []record.Record
		want      map[string][]uint64
		wantUndec []string
	}{
		{
			name: "keyframe and deltas",
			recs: mkAll(
				rspec{rid: "kf", sess: "s", arr: 0, ids: a},
				rspec{rid: "d1", sess: "s", arr: 10, ids: d1, anchor: 4},
				rspec{rid: "d2", sess: "s", arr: 20, ids: d2, anchor: 6},
				rspec{rid: "d3", sess: "s", arr: 30, ids: d3, anchor: 2},
			),
			want: map[string][]uint64{"kf": a, "d1": d1, "d2": d2, "d3": d3},
		},
		{
			name: "anchor arrives after the delta",
			recs: mkAll(
				rspec{rid: "kf", sess: "s", arr: 0, ids: a},
				rspec{rid: "d4", sess: "s", arr: 5, ids: d4, anchor: 6},
				rspec{rid: "d1", sess: "s", arr: 10, ids: d1, anchor: 4},
			),
			want: map[string][]uint64{"kf": a, "d1": d1, "d4": d4},
		},
		{
			name: "undecodable anchors",
			recs: append(mkAll(
				rspec{rid: "kf", sess: "s", arr: 0, ids: a},
				rspec{rid: "other-gen", sess: "s", gen: 2, arr: 10, ids: d1, anchor: 4},
				rspec{rid: "other-rep", sess: "s", rep: "rep-b", arr: 10, ids: d1, anchor: 4},
				rspec{rid: "other-sess", sess: "t", arr: 10, ids: d1, anchor: 4},
				rspec{rid: "no-sess", arr: 10, ids: d1, anchor: 4},
			), missing),
			want:      map[string][]uint64{"kf": a},
			wantUndec: []string{"missing", "no-sess", "other-gen", "other-rep", "other-sess"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ds, bad := decodeAll(ptrs(tc.recs))
			got := map[string][]uint64{}
			for _, d := range ds {
				got[d.rec.Rid] = d.ids
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("decoded ids (-want +got):\n%s", diff)
			}
			var undec []string
			for _, r := range bad {
				undec = append(undec, r.Rid)
			}
			sort.Strings(undec)
			if diff := cmp.Diff(tc.wantUndec, undec, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("undecodable (-want +got):\n%s", diff)
			}
		})
	}
}

// chains runs decode and lineage over one session and returns the rids of
// each chain, ordered by chain root arrival.
func chains(t *testing.T, recs []record.Record, tolMs int64) [][]string {
	t.Helper()
	ds, bad := decodeAll(ptrs(recs))
	if len(bad) > 0 {
		t.Fatalf("%d undecodable records", len(bad))
	}
	sort.SliceStable(ds, func(i, j int) bool { return lessRec(ds[i].rec, ds[j].rec) })
	parent := linkParents(ds, tolMs)
	root := make([]int, len(ds))
	var out [][]string
	pos := map[int]int{}
	for i, d := range ds {
		if parent[i] < 0 {
			root[i] = i
			pos[i] = len(out)
			out = append(out, nil)
		} else {
			root[i] = root[parent[i]]
		}
		out[pos[root[i]]] = append(out[pos[root[i]]], d.rec.Rid)
	}
	return out
}

func TestLinkParents(t *testing.T) {
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
		name  string
		recs  []record.Record
		tolMs int64
		want  [][]string
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
			tol := tc.tolMs
			if tol == 0 {
				tol = DefaultSkewTol.Milliseconds()
			}
			if diff := cmp.Diff(tc.want, chains(t, tc.recs, tol)); diff != "" {
				t.Errorf("chains (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLinkParentsSkew(t *testing.T) {
	g := &idGen{}
	p := g.fresh(4)
	c := g.extend(p, 4, 2)
	linked := [][]string{{"p", "c"}}
	split := [][]string{{"p"}, {"c"}}

	tests := []struct {
		name string
		// gapMs is child arrival minus parent end.
		gapMs int64
		tolMs int64
		want  [][]string
	}{
		{name: "child 50ms after parent end", gapMs: 50, tolMs: 250, want: linked},
		{name: "child 50ms before parent end", gapMs: -50, tolMs: 250, want: linked},
		{name: "child 300ms before parent end", gapMs: -300, tolMs: 250, want: split},
		{name: "child 50ms before parent end without tolerance", gapMs: -50, tolMs: 0, want: split},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recs := mkAll(
				rspec{rid: "p", sess: "s", arr: 0, api: 1000, ids: p},
				rspec{rid: "c", sess: "s", arr: 1000 + tc.gapMs, api: 1000, ids: c, anchor: 4},
			)
			if diff := cmp.Diff(tc.want, chains(t, recs, tc.tolMs)); diff != "" {
				t.Errorf("chains (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRetryDedup(t *testing.T) {
	g := &idGen{}
	a := g.fresh(4)
	b := g.fresh(4)

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
			name: "5xx retried within window",
			recs: mkAll(
				rspec{rid: "r1", sess: "s", arr: 0, api: 100, ids: a, status: 503},
				rspec{rid: "r2", sess: "s", arr: 1500, api: 100, ids: a},
			),
			want:        []row{{"r2", true}},
			wantRetries: 1,
		},
		{
			name: "client disconnect retried",
			recs: mkAll(
				rspec{rid: "r1", sess: "s", arr: 0, api: 100, ids: a, cause: record.CauseClientDisconnect},
				rspec{rid: "r2", sess: "s", arr: 2000, api: 100, ids: a},
			),
			want:        []row{{"r2", true}},
			wantRetries: 1,
		},
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
		{
			name: "different prompt",
			recs: mkAll(
				rspec{rid: "r1", sess: "s", arr: 0, api: 100, ids: a, status: 503},
				rspec{rid: "r2", sess: "s", arr: 1000, api: 100, ids: b},
			),
			want: []row{{"r1", false}, {"r2", false}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := Build(tc.recs, Options{SkewTol: DefaultSkewTol, Seed: "test"})
			if err != nil {
				t.Fatal(err)
			}
			var got []row
			for _, r := range res.Sidecar {
				got = append(got, row{r.Rid, r.Retry})
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("sidecar (-want +got):\n%s", diff)
			}
			if res.Stats.Retries != tc.wantRetries {
				t.Errorf("Stats.Retries = %d, want %d", res.Stats.Retries, tc.wantRetries)
			}
			if got := res.Traces[0].Totals.LLMD.Retries; got != tc.wantRetries {
				t.Errorf("totals retries = %d, want %d", got, tc.wantRetries)
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
	res, err := Build(recs, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	got := make([]float64, 0, len(res.Traces[0].Requests))
	for _, e := range res.Traces[0].Requests {
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
	a := g.fresh(3)
	b := g.fresh(3)
	c := g.fresh(3)
	d := g.fresh(3)
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
		wantTraces [][]string
		wantStats  Stats
	}{
		{
			name:       "estimate refused",
			opts:       DefaultOptions(),
			wantTraces: [][]string{{"x1"}, {"x2"}, {"n1"}, {"n2"}},
			wantStats:  Stats{Invalid: 1, Duplicates: 1, Estimate: 1, Requests: 4, Traces: 4},
		},
		{
			name:       "estimate allowed",
			opts:       Options{AllowEstimate: true},
			wantTraces: [][]string{{"x1"}, {"x2"}, {"n1"}, {"n2"}, {"e1"}},
			wantStats:  Stats{Invalid: 1, Duplicates: 1, Requests: 5, Traces: 5},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := Build(recs, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			got := make([][]string, len(res.Traces))
			ids := map[string]bool{}
			for _, r := range res.Sidecar {
				i := traceIndex(res.Traces, r.Trace)
				got[i] = append(got[i], r.Rid)
				ids[r.Trace] = true
			}
			if diff := cmp.Diff(tc.wantTraces, got); diff != "" {
				t.Errorf("traces (-want +got):\n%s", diff)
			}
			if len(ids) != len(res.Traces) {
				t.Errorf("trace ids not unique: %d ids for %d traces", len(ids), len(res.Traces))
			}
			if diff := cmp.Diff(tc.wantStats, res.Stats); diff != "" {
				t.Errorf("stats (-want +got):\n%s", diff)
			}
		})
	}
}

func traceIndex(traces []Trace, id string) int {
	for i := range traces {
		if traces[i].ID == id {
			return i
		}
	}
	return -1
}

func TestTraceIDs(t *testing.T) {
	g := &idGen{}
	recs := mkAll(rspec{rid: "r1", sess: "s", arr: 0, api: 100, ids: g.fresh(2)})
	seeded := func(seed string) string {
		res, err := Build(recs, Options{Seed: seed})
		if err != nil {
			t.Fatal(err)
		}
		return res.Traces[0].ID
	}
	a1, a2, b := seeded("a"), seeded("a"), seeded("b")
	if a1 != a2 {
		t.Error("seeded trace ids differ across runs")
	}
	if a1 == b {
		t.Error("different seeds produced the same trace id")
	}
	r1, r2 := seeded(""), seeded("")
	if r1 == r2 || len(r1) != 2*traceIDBytes {
		t.Errorf("random trace ids %q, %q: want distinct %d-char hex", r1, r2, 2*traceIDBytes)
	}
}
