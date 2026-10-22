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
	"cmp"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
)

const (
	// retryWindowMs bounds the arrival gap between a failed attempt and its
	// retry.
	retryWindowMs = 2000
	// minMatchBlocks is the minimum shared prefix for a lineage link.
	minMatchBlocks = 2
)

// decoded is a record with its full block id list resolved.
type decoded struct {
	rec   *record.Record
	ids   []uint64
	retry bool
}

// decode resolves the block ids of one session's records, sorted by
// arrival, and returns the decoded ones in the same order. A delta anchors
// on a record of the same replica and file generation. That anchor is the
// last record written, which can arrive after the delta, so unresolved
// records are retried until a pass makes no progress.
func decode(recs []*record.Record) (out []*decoded, undecodable int) {
	ids := make([][]uint64, len(recs))
	ok := make([]bool, len(recs))
	for progress := true; progress; {
		progress = false
		for i, r := range recs {
			if !ok[i] {
				ids[i], ok[i] = resolve(r, recs, ids, ok)
				progress = progress || ok[i]
			}
		}
	}
	for i, r := range recs {
		if ok[i] {
			out = append(out, &decoded{rec: r, ids: ids[i]})
		}
	}
	return out, len(recs) - len(out)
}

func resolve(r *record.Record, recs []*record.Record, ids [][]uint64, ok []bool) ([]uint64, bool) {
	newIDs, err := record.DecodeIDs(r.New)
	if err != nil || r.Anchor == nil || r.Anchor.N == 0 {
		return newIDs, err == nil
	}
	n := r.Anchor.N
	h, err := strconv.ParseUint(r.Anchor.H, 16, 64)
	if err != nil || n < 0 {
		return nil, false
	}
	// Block ids are chained hashes, so matching block n-1 implies the whole
	// prefix [0, n) matches.
	for j, a := range recs {
		if ok[j] && a.Rep == r.Rep && a.Gen == r.Gen && len(ids[j]) >= n && ids[j][n-1] == h {
			return append(slices.Clip(ids[j][:n]), newIDs...), true
		}
	}
	return nil, false
}

// groupSess returns the session grouping key. Records without a session are
// keyed by their own rid so that each forms its own group.
func groupSess(r *record.Record) string {
	if r.Sess == nil {
		return "rid:" + r.Rid
	}
	return "sess:" + *r.Sess
}

// cmpRec orders records by arrival.
func cmpRec(a, b *record.Record) int {
	return cmp.Or(cmp.Compare(a.ArrMs, b.ArrMs), strings.Compare(a.Rid, b.Rid))
}

// dedupRetries drops a failed attempt when the next request in the session
// repeats it within retryWindowMs, and marks the survivor as a retry. ds
// must be sorted by arrival.
func dedupRetries(ds []*decoded) ([]*decoded, int) {
	out := make([]*decoded, 0, len(ds))
	for _, d := range ds {
		if k := len(out) - 1; k >= 0 {
			p := out[k].rec
			failed := p.Cause != record.CauseNatural || p.Status == http.StatusTooManyRequests ||
				p.Status >= http.StatusInternalServerError
			if failed && d.rec.ArrMs-p.ArrMs <= retryWindowMs && slices.Equal(out[k].ids, d.ids) {
				d.retry = true
				out[k] = d
				continue
			}
		}
		out = append(out, d)
	}
	return out, len(ds) - len(out)
}

// linkParents returns, for each request, the index of its lineage parent or
// -1 for a chain root. ds must be sorted by arrival.
//
// A parent must have completed by the child's arrival (within skewTolMs) and
// the child must extend it: the shared prefix covers all but at most the
// last full block of the parent, and at least minMatchBlocks blocks. Each
// parent continues into at most one child, assigned greedily in child
// arrival order, so parallel requests that share a prompt start separate
// chains. Among candidates the longest match wins, then the latest end, then
// the smallest rid.
func linkParents(ds []*decoded, skewTolMs int64) []int {
	end := func(j int) int64 { return ds[j].rec.ArrMs + ds[j].rec.APIMs }
	parent := make([]int, len(ds))
	taken := make([]bool, len(ds))
	for i, c := range ds {
		best, bestMatch := -1, 0
		for j := range i {
			p := ds[j].rec
			if taken[j] || end(j) > c.rec.ArrMs+skewTolMs {
				continue
			}
			m := commonPrefix(ds[j].ids, c.ids)
			if m < max(minMatchBlocks, p.PromptTok/record.BlockSizeTokens-1) {
				continue
			}
			if best < 0 || cmp.Or(cmp.Compare(m, bestMatch), cmp.Compare(end(j), end(best)),
				strings.Compare(ds[best].rec.Rid, p.Rid)) > 0 {
				best, bestMatch = j, m
			}
		}
		parent[i] = best
		if best >= 0 {
			taken[best] = true
		}
	}
	return parent
}

// commonPrefix returns the length of the longest common prefix of a and b.
// Block ids are chained hashes, so a[k] == b[k] implies a[:k] == b[:k] and
// a binary search finds the length in O(log n) comparisons, which keeps
// linkParents' all-pairs scan cheap for sessions of thousands of requests
// with prompts of thousands of blocks.
func commonPrefix(a, b []uint64) int {
	n := min(len(a), len(b))
	return sort.Search(n, func(k int) bool { return a[k] != b[k] })
}
