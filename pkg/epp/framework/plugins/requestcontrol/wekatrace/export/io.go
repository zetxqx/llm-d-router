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
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
)

// Export reads the records in dir and builds traces from them.
func Export(dir string, opts Options) (*Result, error) {
	recs, malformed, err := Read(dir)
	if err != nil {
		return nil, err
	}
	res, err := Build(recs, opts)
	if res != nil {
		res.Stats.Malformed = malformed
	}
	return res, err
}

// Read parses every *.jsonl file in dir, in name order, and returns the
// records and the number of lines that are not valid JSON records. Record
// invariants are checked by Build.
func Read(dir string) (recs []record.Record, malformed int, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, 0, err
		}
		for line := range bytes.Lines(data) {
			if line = bytes.TrimSpace(line); len(line) == 0 {
				continue
			}
			var rec record.Record
			if json.Unmarshal(line, &rec) != nil {
				malformed++
				continue
			}
			recs = append(recs, rec)
		}
	}
	return recs, malformed, nil
}

// MarshalJSON encodes whichever of Request or Subagent is set.
func (e Entry) MarshalJSON() ([]byte, error) {
	if e.Subagent != nil {
		return json.Marshal(e.Subagent)
	}
	return json.Marshal(e.Request)
}

// WriteTraces writes each trace to dir/<id>.json, creating dir if needed.
func WriteTraces(dir string, traces []Trace) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for i := range traces {
		if err := writeJSON(filepath.Join(dir, traces[i].ID+".json"), traces[i:i+1]); err != nil {
			return err
		}
	}
	return nil
}

// WriteSidecar writes one JSON line per row to path.
func WriteSidecar(path string, rows []SidecarRow) error {
	return writeJSON(path, rows)
}

// writeJSON writes one JSON line per element of vs to path.
func writeJSON[T any](path string, vs []T) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for i := range vs {
		if err := enc.Encode(&vs[i]); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	return w.Flush()
}
