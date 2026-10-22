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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// MarshalJSON encodes whichever of Request or Subagent is set.
func (e Entry) MarshalJSON() ([]byte, error) {
	switch {
	case e.Subagent != nil && e.Request == nil:
		return json.Marshal(e.Subagent)
	case e.Request != nil && e.Subagent == nil:
		return json.Marshal(e.Request)
	default:
		return nil, errors.New("entry must set exactly one of Request and Subagent")
	}
}

// WriteTraces writes each trace to dir/<id>.json, creating dir if needed.
func WriteTraces(dir string, traces []Trace) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for i := range traces {
		if err := writeJSON(filepath.Join(dir, traces[i].ID+".json"), func(enc *json.Encoder) error {
			return enc.Encode(&traces[i])
		}); err != nil {
			return err
		}
	}
	return nil
}

// WriteSidecar writes one JSON line per row to path.
func WriteSidecar(path string, rows []SidecarRow) error {
	return writeJSON(path, func(enc *json.Encoder) error {
		for i := range rows {
			if err := enc.Encode(&rows[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeJSON(path string, encode func(*json.Encoder) error) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := encode(enc); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return w.Flush()
}
