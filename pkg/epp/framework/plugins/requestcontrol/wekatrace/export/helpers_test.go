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
	rid     string
	sess    string // empty means no session
	kid     string
	rep     string
	gen     uint64
	arr     int64
	api     int64
	ttft    int64 // 0 means absent
	prompt  int   // 0 means 64 per block
	ids     []uint64
	anchor  int // 0 means keyframe
	stream  bool
	cause   string
	status  int
	model   string
	backend string
	out     int
}

func mk(s rspec) record.Record {
	r := record.Record{
		V:          record.Version,
		Kid:        or(s.kid, "k1"),
		Rid:        s.rid,
		Rep:        or(s.rep, "rep-a"),
		Gen:        s.gen,
		ArrMs:      s.arr,
		Model:      or(s.model, "m1"),
		Stream:     s.stream,
		TokBackend: or(s.backend, "vllm"),
		PromptTok:  s.prompt,
		Blocks:     len(s.ids),
		New:        record.EncodeIDs(s.ids[s.anchor:]),
		Kf:         s.anchor == 0,
		OutTok:     s.out,
		UsageSrc:   record.UsageSrcServer,
		APIMs:      s.api,
		Status:     s.status,
		Cause:      or(s.cause, record.CauseNatural),
	}
	if s.sess != "" {
		sess := s.sess
		r.Sess = &sess
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
		ttft := s.ttft
		r.TTFTMs = &ttft
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

func or(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
