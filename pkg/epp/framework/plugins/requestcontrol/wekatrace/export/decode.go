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
	"strconv"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
)

// decoded is a record with its full block id list resolved.
type decoded struct {
	rec   *record.Record
	ids   []uint64
	retry bool
}

// streamKey identifies the records that may anchor on each other: one
// session within one file generation of one replica.
type streamKey struct {
	rep  string
	gen  uint64
	sess string
}

// decodeAll resolves every record's block id list. A delta whose anchor is
// not found among the decoded records of its stream is returned as
// undecodable.
func decodeAll(recs []*record.Record) (out []*decoded, undecodable []*record.Record) {
	streams := map[streamKey][]*record.Record{}
	var keys []streamKey
	for _, r := range recs {
		k := streamKey{rep: r.Rep, gen: r.Gen, sess: groupSess(r)}
		if _, ok := streams[k]; !ok {
			keys = append(keys, k)
		}
		streams[k] = append(streams[k], r)
	}
	for _, k := range keys {
		ok, bad := decodeStream(streams[k])
		out = append(out, ok...)
		undecodable = append(undecodable, bad...)
	}
	return out, undecodable
}

// decodeStream decodes records in arrival order. Records whose anchor has
// not been decoded yet are retried after each pass, since the anchor is the
// last record written, which can arrive after the record that references it.
func decodeStream(recs []*record.Record) ([]*decoded, []*record.Record) {
	sort.SliceStable(recs, func(i, j int) bool { return lessRec(recs[i], recs[j]) })
	var done []*decoded
	pending := recs
	for len(pending) > 0 {
		var next []*record.Record
		for _, r := range pending {
			if ids, ok := resolve(r, done); ok {
				done = append(done, &decoded{rec: r, ids: ids})
			} else {
				next = append(next, r)
			}
		}
		if len(next) == len(pending) {
			return done, next
		}
		pending = next
	}
	return done, nil
}

func resolve(r *record.Record, done []*decoded) ([]uint64, bool) {
	newIDs, err := record.DecodeIDs(r.New)
	if err != nil {
		return nil, false
	}
	if r.Anchor == nil || r.Anchor.N == 0 {
		return newIDs, true
	}
	n := r.Anchor.N
	h, err := strconv.ParseUint(r.Anchor.H, 16, 64)
	if err != nil || n < 0 {
		return nil, false
	}
	// Block ids are chained hashes, so matching block n-1 implies the whole
	// prefix [0, n) matches.
	for i := len(done) - 1; i >= 0; i-- {
		ids := done[i].ids
		if len(ids) >= n && ids[n-1] == h {
			full := make([]uint64, 0, n+len(newIDs))
			full = append(full, ids[:n]...)
			return append(full, newIDs...), true
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

func lessRec(a, b *record.Record) bool {
	if a.ArrMs != b.ArrMs {
		return a.ArrMs < b.ArrMs
	}
	return a.Rid < b.Rid
}
