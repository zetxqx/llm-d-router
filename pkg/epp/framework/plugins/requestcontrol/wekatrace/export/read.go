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
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
)

// Export reads the records in dir and builds traces from them.
func Export(dir string, opts Options) (*Result, error) {
	recs, st, err := Read(dir)
	if err != nil {
		return nil, err
	}
	res, err := Build(recs, opts)
	if err != nil {
		return nil, err
	}
	res.Stats.Files, res.Stats.Lines, res.Stats.Malformed = st.Files, st.Lines, st.Malformed
	return res, nil
}

// Read parses every *.jsonl and *.jsonl.gz file in dir, in name order.
// Lines that are not valid JSON records are counted in Stats.Malformed and
// skipped; record invariants are checked by Build.
func Read(dir string) ([]record.Record, Stats, error) {
	var stats Stats
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, stats, err
	}
	var names []string
	for _, e := range entries {
		n := e.Name()
		if !e.IsDir() && (strings.HasSuffix(n, ".jsonl") || strings.HasSuffix(n, ".jsonl.gz")) {
			names = append(names, n)
		}
	}
	sort.Strings(names)

	var recs []record.Record
	for _, n := range names {
		if recs, err = readFile(filepath.Join(dir, n), recs, &stats); err != nil {
			return nil, stats, fmt.Errorf("read %s: %w", n, err)
		}
		stats.Files++
	}
	return recs, stats, nil
}

func readFile(path string, recs []record.Record, stats *Stats) ([]record.Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return recs, err
	}
	defer f.Close()

	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return recs, err
		}
		defer gz.Close()
		r = gz
	}

	// bufio.Reader has no line length limit; keyframes of long prompts
	// exceed bufio.Scanner's default token size.
	br := bufio.NewReader(r)
	for {
		line, err := br.ReadBytes('\n')
		if line = bytes.TrimSpace(line); len(line) > 0 {
			stats.Lines++
			var rec record.Record
			if json.Unmarshal(line, &rec) != nil {
				stats.Malformed++
			} else {
				recs = append(recs, rec)
			}
		}
		if errors.Is(err, io.EOF) {
			return recs, nil
		}
		if err != nil {
			return recs, err
		}
	}
}
