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

// Package record defines the per-request shape record written by the
// weka-trace-recorder plugin and read by the offline weka-export tool.
//
// A record carries no prompt or completion content. Block ids are keyed PRF
// values derived per session, so equal content in different sessions yields
// unrelated ids. See docs/proposals/weka-trace-recorder/design.md.
package record

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
)

// Version is the current record schema version. Readers ignore unknown
// fields; only breaking changes bump the version.
const Version = 1

// BlockSizeTokens is the fixed WekaTrace block size.
const BlockSizeTokens = 64

// Anchor references the longest full-block prefix this replica already wrote
// for the session in the same file generation.
type Anchor struct {
	// N is the number of shared leading full blocks.
	N int `json:"n"`
	// H is the hex PRF id of block N-1.
	H string `json:"h"`
}

// Record is one line of the recorder's JSONL output. All times are
// milliseconds; the exporter converts them to seconds for WekaTrace.
type Record struct {
	V   int    `json:"v"`
	Kid string `json:"kid"`
	// Rid is the PRF of the EPP request UUID; it is the dedup key.
	Rid string `json:"rid"`
	// Rep is the PRF of the EPP replica (pod) name.
	Rep string `json:"rep"`
	// Gen is the file generation the record was encoded for. Deltas only
	// reference records with the same (Rep, Gen).
	Gen uint64 `json:"gen"`
	// Sess is the PRF of the session id; nil when the request had none.
	Sess  *string `json:"sess"`
	ArrMs int64   `json:"arr_ms"`

	API        string `json:"api,omitempty"`
	Model      string `json:"model"`
	Stream     bool   `json:"stream"`
	TokBackend string `json:"tok_backend"`
	PromptTok  int    `json:"prompt_tok"`
	// Blocks is ceil(PromptTok/64) over the hashed prompt, including a
	// partial tail block. Invariant: Blocks == Anchor.N + len(NewIDs).
	Blocks int     `json:"blocks"`
	Anchor *Anchor `json:"anchor,omitempty"`
	// New is the little-endian uint64 PRF ids of blocks [Anchor.N, Blocks),
	// standard base64. Keyframes (Kf) carry all blocks.
	New string `json:"new"`
	Kf  bool   `json:"kf"`

	CachedTok *int   `json:"cached_tok,omitempty"`
	OutTok    int    `json:"out_tok"`
	UsageSrc  string `json:"usage_src"`
	Events    int    `json:"events"`
	MaxTokens *int64 `json:"max_tokens,omitempty"`

	TTFTMs  *int64 `json:"ttft_ms,omitempty"`
	APIMs   int64  `json:"api_ms"`
	QueueMs *int64 `json:"queue_ms,omitempty"`
	Status  int    `json:"status,omitempty"`
	Cause   string `json:"cause"`
	Ep      string `json:"ep,omitempty"`
	Profile string `json:"profile,omitempty"`
}

// Usage source values.
const (
	UsageSrcServer = "server"
	UsageSrcEvents = "events"
	UsageSrcNone   = "none"
)

// Cause values.
const (
	CauseNatural          = "natural"
	CauseClientDisconnect = "client-disconnect"
)

// TokBackendEstimate marks block ids computed from an approximate tokenizer.
const TokBackendEstimate = "estimate"

// EncodeIDs packs ids as little-endian uint64 and returns standard base64.
func EncodeIDs(ids []uint64) string {
	buf := make([]byte, 8*len(ids))
	for i, id := range ids {
		binary.LittleEndian.PutUint64(buf[i*8:], id)
	}
	return base64.StdEncoding.EncodeToString(buf)
}

// DecodeIDs reverses EncodeIDs.
func DecodeIDs(s string) ([]uint64, error) {
	buf, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode ids: %w", err)
	}
	if len(buf)%8 != 0 {
		return nil, errors.New("decode ids: length is not a multiple of 8")
	}
	ids := make([]uint64, len(buf)/8)
	for i := range ids {
		ids[i] = binary.LittleEndian.Uint64(buf[i*8:])
	}
	return ids, nil
}

// HexID formats a block id the way Anchor.H stores it.
func HexID(id uint64) string { return fmt.Sprintf("%016x", id) }

// Validate checks the record's internal invariants.
func (r *Record) Validate() error {
	if r.V != Version {
		return fmt.Errorf("unsupported record version %d", r.V)
	}
	ids, err := DecodeIDs(r.New)
	if err != nil {
		return err
	}
	n := 0
	if r.Anchor != nil {
		if r.Kf {
			return errors.New("keyframe must not carry an anchor")
		}
		n = r.Anchor.N
	} else if !r.Kf && r.Blocks > 0 {
		return errors.New("non-keyframe record without anchor")
	}
	if n+len(ids) != r.Blocks {
		return fmt.Errorf("blocks %d != anchor %d + new %d", r.Blocks, n, len(ids))
	}
	return nil
}
