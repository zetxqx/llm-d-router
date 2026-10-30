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

const (
	// DefaultSkewTol is the lineage clock skew tolerance between replicas.
	DefaultSkewTol = 250 * time.Millisecond
	// HashIDScope is the WekaTrace hash_id_scope of every exported trace.
	HashIDScope = "local"

	// WekaTrace request types.
	TypeStream   = "s"
	TypeNormal   = "n"
	TypeSubagent = "subagent"

	// Subagent status values.
	StatusCompleted = "completed"
	StatusCancelled = "cancelled"
	StatusError     = "error"

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

// Stats counts the input that was dropped or merged.
type Stats struct {
	Malformed   int
	Invalid     int
	Duplicates  int
	Undecodable int
	Estimate    int
	Retries     int
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
	groups := map[traceKey][]*record.Record{}
	for i := range recs {
		r := &recs[i]
		switch {
		case r.Validate() != nil:
			st.Invalid++
		case seen[r.Rid]:
			st.Duplicates++
		default:
			seen[r.Rid] = true
			k := traceKey{kid: r.Kid, sess: groupSess(r)}
			groups[k] = append(groups[k], r)
		}
	}

	type group struct {
		key         traceKey
		ds          []*decoded
		undecodable int
	}
	ordered := make([]group, 0, len(groups))
	for k, rs := range groups {
		slices.SortFunc(rs, cmpRec)
		ds, bad := decode(rs)
		st.Undecodable += bad
		n := len(ds)
		ds = slices.DeleteFunc(ds, func(d *decoded) bool {
			return d.rec.TokBackend == record.TokBackendEstimate && !opts.AllowEstimate
		})
		st.Estimate += n - len(ds)
		if len(ds) > 0 {
			ordered = append(ordered, group{k, ds, bad})
		}
	}
	slices.SortFunc(ordered, func(a, b group) int { return cmpRec(a.ds[0].rec, b.ds[0].rec) })

	for _, g := range ordered {
		ds, retries := dedupRetries(g.ds)
		id, err := traceID(opts.Seed, g.key, ds[0].rec.Rid)
		if err != nil {
			return nil, err
		}
		tr, rows := buildTrace(id, ds, linkParents(ds, opts.SkewTol.Milliseconds()))
		tr.Totals.LLMD.DroppedUndecodable = g.undecodable
		tr.Totals.LLMD.Retries = retries
		res.Traces = append(res.Traces, tr)
		res.Sidecar = append(res.Sidecar, rows...)
		st.Retries += retries
	}
	return res, nil
}

// buildTrace assembles one trace from requests sorted by arrival. The chain
// containing the first request is the main chain; every other chain becomes
// a subagent entry placed at its first request's arrival.
func buildTrace(id string, ds []*decoded, parent []int) (Trace, []SidecarRow) {
	tr := Trace{ID: id, BlockSize: record.BlockSizeTokens, HashIDScope: HashIDScope}
	llmd := &tr.Totals.LLMD
	*llmd = LLMDTotals{TokBackend: []string{}, UsageSrc: map[string]int{}, Records: len(ds)}
	reps := map[string]bool{}
	dense := map[uint64]int{}
	// entry[i] is the position of request i's subagent entry, or -1 when
	// request i is on the main chain.
	entry := make([]int, len(ds))
	subagents := 0
	rows := make([]SidecarRow, 0, len(ds))
	for i, d := range ds {
		r := d.rec
		tr.Models = appendUnique(tr.Models, r.Model)
		llmd.TokBackend = appendUnique(llmd.TokBackend, r.TokBackend)
		llmd.UsageSrc[r.UsageSrc]++
		reps[r.Rep] = true

		req := Request{
			T:       msToSec(r.ArrMs - ds[0].rec.ArrMs),
			Type:    TypeNormal,
			Model:   r.Model,
			In:      r.PromptTok,
			Out:     r.OutTok,
			HashIDs: make([]int, len(d.ids)),
			APITime: msToSec(r.APIMs),
		}
		for k, h := range d.ids {
			v, ok := dense[h]
			if !ok {
				v = len(dense)
				dense[h] = v
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
		p := parent[i]
		if p >= 0 {
			pr := ds[p].rec
			req.ThinkTime = msToSec(max(0, r.ArrMs-(pr.ArrMs+pr.APIMs)))
		}

		row := SidecarRow{
			Trace: id, Rid: r.Rid, CachedTok: r.CachedTok, QueueMs: r.QueueMs, Ep: r.Ep,
			Profile: r.Profile, Cause: r.Cause, Status: r.Status, UsageSrc: r.UsageSrc,
			Retry: d.retry, ArrivalMinute: r.ArrMs - r.ArrMs%minuteMs,
		}
		switch {
		case i == 0 || p >= 0 && entry[p] < 0:
			entry[i] = -1
			row.Index = len(tr.Requests)
			tr.Requests = append(tr.Requests, Entry{Request: &req})
		case p >= 0:
			entry[i] = entry[p]
		default:
			entry[i] = len(tr.Requests)
			subagents++
			tr.Requests = append(tr.Requests, Entry{Subagent: &Subagent{
				T:            req.T,
				Type:         TypeSubagent,
				AgentID:      fmt.Sprintf("agent_%03d", subagents),
				SubagentType: subagentTypeUnknown,
				Status:       StatusCompleted,
			}})
		}
		if e := entry[i]; e >= 0 {
			sa := tr.Requests[e].Subagent
			sub := len(sa.Requests)
			row.Index, row.AgentID, row.SubIndex = e, sa.AgentID, &sub
			sa.Requests = append(sa.Requests, req)
			sa.Models = appendUnique(sa.Models, r.Model)
			switch r.Cause {
			case record.CauseNatural:
			case record.CauseClientDisconnect:
				sa.Status = StatusCancelled
			default:
				if sa.Status == StatusCompleted {
					sa.Status = StatusError
				}
			}
		}
		rows = append(rows, row)
	}
	llmd.Replicas = len(reps)
	slices.Sort(llmd.TokBackend)
	// Group each subagent's rows at its entry position.
	sort.SliceStable(rows, func(a, b int) bool { return rows[a].Index < rows[b].Index })
	return tr, rows
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
