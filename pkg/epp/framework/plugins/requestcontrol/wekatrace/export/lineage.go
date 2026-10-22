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
	"net/http"
	"slices"
	"sort"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
)

const (
	// retryWindowMs bounds the arrival gap between a failed attempt and its
	// retry.
	retryWindowMs = 2000
	// minMatchBlocks is the minimum shared prefix for a lineage link.
	minMatchBlocks = 2
)

// dedupRetries drops a failed attempt when the next request in the session
// repeats it within retryWindowMs, and marks the survivor as a retry. ds
// must be sorted by arrival.
func dedupRetries(ds []*decoded) ([]*decoded, int) {
	out := make([]*decoded, 0, len(ds))
	n := 0
	for _, d := range ds {
		if k := len(out); k > 0 && isRetryOf(out[k-1], d) {
			d.retry = true
			out[k-1] = d
			n++
			continue
		}
		out = append(out, d)
	}
	return out, n
}

func isRetryOf(prev, cur *decoded) bool {
	p := prev.rec
	failed := p.Cause != record.CauseNatural || p.Status == http.StatusTooManyRequests ||
		p.Status >= http.StatusInternalServerError
	return failed && cur.rec.ArrMs-p.ArrMs <= retryWindowMs && slices.Equal(prev.ids, cur.ids)
}

// linkParents returns, for each request, the index of its lineage parent or
// -1 for a chain root. ds must be sorted by arrival.
//
// A parent must have completed by the child's arrival (within skewTolMs) and
// the child must extend it: the shared prefix covers all but at most the
// last full block of the parent, and at least minMatchBlocks blocks. Each
// parent continues into at most one child, assigned greedily in child
// arrival order, so parallel requests that share a prompt start separate
// chains.
func linkParents(ds []*decoded, skewTolMs int64) []int {
	parent := make([]int, len(ds))
	taken := make([]bool, len(ds))
	type cand struct{ j, match int }
	var cands []cand
	for i, c := range ds {
		parent[i] = -1
		cands = cands[:0]
		for j := range i {
			p := ds[j].rec
			if p.ArrMs+p.APIMs > c.rec.ArrMs+skewTolMs {
				continue
			}
			need := max(minMatchBlocks, p.PromptTok/record.BlockSizeTokens-1)
			if m := commonPrefix(ds[j].ids, c.ids); m >= need {
				cands = append(cands, cand{j, m})
			}
		}
		sort.Slice(cands, func(a, b int) bool {
			pa, pb := ds[cands[a].j].rec, ds[cands[b].j].rec
			if cands[a].match != cands[b].match {
				return cands[a].match > cands[b].match
			}
			if ea, eb := pa.ArrMs+pa.APIMs, pb.ArrMs+pb.APIMs; ea != eb {
				return ea > eb
			}
			return pa.Rid < pb.Rid
		})
		for _, cd := range cands {
			if !taken[cd.j] {
				taken[cd.j] = true
				parent[i] = cd.j
				break
			}
		}
	}
	return parent
}

// commonPrefix returns the length of the longest common prefix of a and b.
// Block ids are chained hashes, so a[k] == b[k] implies a[:k] == b[:k] and
// the length is found by binary search.
func commonPrefix(a, b []uint64) int {
	n := min(len(a), len(b))
	return sort.Search(n, func(k int) bool { return a[k] != b[k] })
}
