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

// Package export converts weka-trace-recorder shape records into WekaTrace
// v7 files as loaded by aiperf, plus a sidecar JSONL with the per-request
// fields WekaTrace has no place for. See
// docs/proposals/weka-trace-recorder/design.md.
package export

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
)

// DefaultSkewTol is the lineage clock skew tolerance between replicas.
const DefaultSkewTol = 250 * time.Millisecond

// HashIDScope is the WekaTrace hash_id_scope of every exported trace.
const HashIDScope = "local"

// WekaTrace request types.
const (
	TypeStream   = "s"
	TypeNormal   = "n"
	TypeSubagent = "subagent"
)

// Subagent status values.
const (
	StatusCompleted = "completed"
	StatusCancelled = "cancelled"
	StatusError     = "error"
)

const (
	subagentTypeUnknown = "unknown"
	traceIDBytes        = 20
	minuteMs            = 60_000
)

// Options controls Build.
type Options struct {
	// AllowEstimate exports records whose block ids come from an
	// approximate tokenizer.
	AllowEstimate bool
	// SkewTol is the tolerance applied when checking that a lineage parent
	// completed before its child arrived.
	SkewTol time.Duration
	// Seed, when set, derives trace ids deterministically from the seed and
	// the trace contents. Empty uses random ids.
	Seed string
}

// DefaultOptions returns the options used by the weka-export command when no
// flags are given.
func DefaultOptions() Options {
	return Options{SkewTol: DefaultSkewTol}
}

// Stats counts what happened to the input.
type Stats struct {
	Files       int
	Lines       int
	Malformed   int
	Invalid     int
	Duplicates  int
	Undecodable int
	Estimate    int
	Retries     int
	Requests    int
	Traces      int
	Subagents   int
}

func (s Stats) String() string {
	return fmt.Sprintf("files=%d lines=%d malformed=%d invalid=%d duplicates=%d undecodable=%d estimate=%d retries=%d requests=%d traces=%d subagents=%d",
		s.Files, s.Lines, s.Malformed, s.Invalid, s.Duplicates, s.Undecodable, s.Estimate, s.Retries, s.Requests, s.Traces, s.Subagents)
}

// Trace is one WekaTrace v7 file.
type Trace struct {
	ID          string   `json:"id"`
	Models      []string `json:"models"`
	BlockSize   int      `json:"block_size"`
	HashIDScope string   `json:"hash_id_scope"`
	Requests    []Entry  `json:"requests"`
	Totals      Totals   `json:"totals"`
}

// Entry is a top-level WekaTrace request: exactly one field is set.
type Entry struct {
	Request  *Request
	Subagent *Subagent
}

// Request is a WekaTrace "n" or "s" request. Times are seconds.
type Request struct {
	T         float64  `json:"t"`
	Type      string   `json:"type"`
	Model     string   `json:"model"`
	In        int      `json:"in"`
	Out       int      `json:"out"`
	HashIDs   []int    `json:"hash_ids"`
	APITime   float64  `json:"api_time"`
	TTFT      *float64 `json:"ttft,omitempty"`
	ThinkTime float64  `json:"think_time"`
}

// Subagent is a WekaTrace "subagent" entry. Fields the recorder cannot
// observe are null so that aiperf derives them from the inner requests.
type Subagent struct {
	T            float64   `json:"t"`
	Type         string    `json:"type"`
	AgentID      string    `json:"agent_id"`
	SubagentType string    `json:"subagent_type"`
	DurationMs   *int64    `json:"duration_ms"`
	TotalTokens  *int64    `json:"total_tokens"`
	ToolUseCount *int64    `json:"tool_use_count"`
	Status       string    `json:"status"`
	Models       []string  `json:"models"`
	Requests     []Request `json:"requests"`
}

// Totals is the free-form WekaTrace totals object.
type Totals struct {
	LLMD LLMDTotals `json:"llm_d"`
}

// LLMDTotals describes how a trace was collected.
type LLMDTotals struct {
	Replicas           int            `json:"replicas"`
	TokBackend         []string       `json:"tok_backend"`
	UsageSrc           map[string]int `json:"usage_src"`
	Records            int            `json:"records"`
	DroppedUndecodable int            `json:"dropped_undecodable"`
	Retries            int            `json:"retries"`
}

// SidecarRow carries per-request fields that WekaTrace does not model.
// Index is the position in the trace's top-level requests; for requests
// inside a subagent, AgentID and SubIndex locate the inner request.
type SidecarRow struct {
	Trace         string `json:"trace"`
	Index         int    `json:"index"`
	AgentID       string `json:"agent_id,omitempty"`
	SubIndex      *int   `json:"sub_index,omitempty"`
	Rid           string `json:"rid"`
	CachedTok     *int   `json:"cached_tok"`
	QueueMs       *int64 `json:"queue_ms"`
	Ep            string `json:"ep"`
	Profile       string `json:"profile"`
	Cause         string `json:"cause"`
	Status        int    `json:"status"`
	UsageSrc      string `json:"usage_src"`
	Retry         bool   `json:"retry"`
	ArrivalMinute int64  `json:"arrival_minute"`
}

// Result is the output of Build.
type Result struct {
	Traces  []Trace
	Sidecar []SidecarRow
	Stats   Stats
}

// traceKey groups records into traces: one session under one key id.
type traceKey struct {
	kid  string
	sess string
}

// Build validates, deduplicates, and decodes recs, reconstructs lineage per
// session, and returns the WekaTrace files in first-arrival order.
func Build(recs []record.Record, opts Options) (*Result, error) {
	res := &Result{}
	st := &res.Stats

	seen := map[string]bool{}
	var valid []*record.Record
	for i := range recs {
		r := &recs[i]
		if r.Validate() != nil {
			st.Invalid++
			continue
		}
		if seen[r.Rid] {
			st.Duplicates++
			continue
		}
		seen[r.Rid] = true
		valid = append(valid, r)
	}

	ds, bad := decodeAll(valid)
	st.Undecodable = len(bad)
	undecodable := map[traceKey]int{}
	for _, r := range bad {
		undecodable[keyOf(r)]++
	}

	groups := map[traceKey][]*decoded{}
	for _, d := range ds {
		if d.rec.TokBackend == record.TokBackendEstimate && !opts.AllowEstimate {
			st.Estimate++
			continue
		}
		k := keyOf(d.rec)
		groups[k] = append(groups[k], d)
	}

	type group struct {
		key traceKey
		ds  []*decoded
	}
	ordered := make([]group, 0, len(groups))
	for k, g := range groups {
		sort.SliceStable(g, func(i, j int) bool { return lessRec(g[i].rec, g[j].rec) })
		ordered = append(ordered, group{k, g})
	}
	sort.Slice(ordered, func(i, j int) bool { return lessRec(ordered[i].ds[0].rec, ordered[j].ds[0].rec) })

	skewTolMs := opts.SkewTol.Milliseconds()
	for _, g := range ordered {
		gds, retries := dedupRetries(g.ds)
		id, err := traceID(opts.Seed, g.key, gds[0].rec.Rid)
		if err != nil {
			return nil, err
		}
		tr, rows := buildTrace(id, gds, linkParents(gds, skewTolMs))
		tr.Totals.LLMD.DroppedUndecodable = undecodable[g.key]
		tr.Totals.LLMD.Retries = retries
		res.Traces = append(res.Traces, tr)
		res.Sidecar = append(res.Sidecar, rows...)
		st.Retries += retries
		st.Requests += len(gds)
		st.Traces++
		for _, e := range tr.Requests {
			if e.Subagent != nil {
				st.Subagents++
			}
		}
	}
	return res, nil
}

func keyOf(r *record.Record) traceKey {
	return traceKey{kid: r.Kid, sess: groupSess(r)}
}

// buildTrace assembles one trace from requests sorted by arrival. The chain
// containing the first request is the main chain; every other chain becomes
// a subagent entry.
func buildTrace(id string, ds []*decoded, parent []int) (Trace, []SidecarRow) {
	t0 := ds[0].rec.ArrMs
	dense := map[uint64]int{}
	reqs := make([]Request, len(ds))
	for i, d := range ds {
		reqs[i] = toRequest(d, t0, dense)
		if p := parent[i]; p >= 0 {
			pr := ds[p].rec
			reqs[i].ThinkTime = msToSec(max(0, d.rec.ArrMs-(pr.ArrMs+pr.APIMs)))
		}
	}

	// Chains are linear because each parent has at most one child, and a
	// parent always precedes its child.
	root := make([]int, len(ds))
	var roots []int
	members := map[int][]int{}
	for i := range ds {
		if parent[i] < 0 {
			root[i] = i
			roots = append(roots, i)
		} else {
			root[i] = root[parent[i]]
		}
		members[root[i]] = append(members[root[i]], i)
	}

	type item struct {
		arrMs int64
		entry Entry
		idx   []int
	}
	items := make([]item, 0, len(members[0])+len(roots)-1)
	for _, i := range members[0] {
		items = append(items, item{ds[i].rec.ArrMs, Entry{Request: &reqs[i]}, []int{i}})
	}
	for n, r := range roots[1:] {
		idx := members[r]
		sa := &Subagent{
			T:            reqs[r].T,
			Type:         TypeSubagent,
			AgentID:      fmt.Sprintf("agent_%03d", n+1),
			SubagentType: subagentTypeUnknown,
			Status:       StatusCompleted,
		}
		for _, i := range idx {
			sa.Requests = append(sa.Requests, reqs[i])
			sa.Models = appendUnique(sa.Models, ds[i].rec.Model)
			switch ds[i].rec.Cause {
			case record.CauseNatural:
			case record.CauseClientDisconnect:
				sa.Status = StatusCancelled
			default:
				if sa.Status == StatusCompleted {
					sa.Status = StatusError
				}
			}
		}
		items = append(items, item{ds[r].rec.ArrMs, Entry{Subagent: sa}, idx})
	}
	sort.SliceStable(items, func(a, b int) bool { return items[a].arrMs < items[b].arrMs })

	tr := Trace{
		ID:          id,
		BlockSize:   record.BlockSizeTokens,
		HashIDScope: HashIDScope,
		Requests:    make([]Entry, len(items)),
		Totals:      Totals{LLMD: LLMDTotals{UsageSrc: map[string]int{}, TokBackend: []string{}, Records: len(ds)}},
	}
	reps := map[string]bool{}
	for _, d := range ds {
		tr.Models = appendUnique(tr.Models, d.rec.Model)
		reps[d.rec.Rep] = true
		tr.Totals.LLMD.TokBackend = appendUnique(tr.Totals.LLMD.TokBackend, d.rec.TokBackend)
		tr.Totals.LLMD.UsageSrc[d.rec.UsageSrc]++
	}
	tr.Totals.LLMD.Replicas = len(reps)
	slices.Sort(tr.Totals.LLMD.TokBackend)

	var rows []SidecarRow
	for pos, it := range items {
		tr.Requests[pos] = it.entry
		for sub, i := range it.idx {
			row := sidecarRow(id, pos, ds[i])
			if it.entry.Subagent != nil {
				row.AgentID = it.entry.Subagent.AgentID
				row.SubIndex = &sub
			}
			rows = append(rows, row)
		}
	}
	return tr, rows
}

func toRequest(d *decoded, t0 int64, dense map[uint64]int) Request {
	r := d.rec
	req := Request{
		T:       msToSec(r.ArrMs - t0),
		Type:    TypeNormal,
		Model:   r.Model,
		In:      r.PromptTok,
		Out:     r.OutTok,
		HashIDs: make([]int, len(d.ids)),
		APITime: msToSec(r.APIMs),
	}
	for k, id := range d.ids {
		v, ok := dense[id]
		if !ok {
			v = len(dense)
			dense[id] = v
		}
		req.HashIDs[k] = v
	}
	if r.Stream {
		req.Type = TypeStream
		if r.TTFTMs != nil {
			ttft := msToSec(*r.TTFTMs)
			req.TTFT = &ttft
		}
	}
	return req
}

func sidecarRow(trace string, pos int, d *decoded) SidecarRow {
	r := d.rec
	return SidecarRow{
		Trace:         trace,
		Index:         pos,
		Rid:           r.Rid,
		CachedTok:     r.CachedTok,
		QueueMs:       r.QueueMs,
		Ep:            r.Ep,
		Profile:       r.Profile,
		Cause:         r.Cause,
		Status:        r.Status,
		UsageSrc:      r.UsageSrc,
		Retry:         d.retry,
		ArrivalMinute: r.ArrMs - r.ArrMs%minuteMs,
	}
}

// traceID returns a random id, or with a seed a hash of the seed and the
// trace's identity so that output is reproducible.
func traceID(seed string, k traceKey, firstRid string) (string, error) {
	if seed == "" {
		b := make([]byte, traceIDBytes)
		if _, err := rand.Read(b); err != nil {
			return "", fmt.Errorf("trace id: %w", err)
		}
		return hex.EncodeToString(b), nil
	}
	sum := sha256.Sum256([]byte(seed + "\x00" + k.kid + "\x00" + k.sess + "\x00" + firstRid))
	return hex.EncodeToString(sum[:traceIDBytes]), nil
}

func msToSec(ms int64) float64 { return float64(ms) / 1000 }

func appendUnique(s []string, v string) []string {
	if slices.Contains(s, v) {
		return s
	}
	return append(s, v)
}
