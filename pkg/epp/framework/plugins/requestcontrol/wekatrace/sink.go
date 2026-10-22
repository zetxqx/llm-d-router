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

package wekatrace

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// partialSuffix marks the file being written; collectors upload only
	// closed files, which carry finalSuffix.
	partialSuffix = ".jsonl.partial"
	finalSuffix   = ".jsonl"
	// diskCheckEvery bounds how often the directory size is recomputed.
	diskCheckEvery = 64
)

// fileSink writes JSONL records into size-bounded files. It never deletes
// files: the collector removes them after upload.
type fileSink struct {
	dir       string
	prefix    string
	fileBytes int64
	highWater int64
	stdout    bool

	generation uint64
	f          *os.File
	written    int64
	sinceCheck int
	full       bool
}

func newFileSink(cfg resolvedConfig, pod string) (*fileSink, error) {
	if err := os.MkdirAll(cfg.sinkPath, 0o750); err != nil {
		return nil, fmt.Errorf("create sink dir: %w", err)
	}
	s := &fileSink{
		dir:       cfg.sinkPath,
		prefix:    sanitize(pod) + "-" + time.Now().UTC().Format("20060102T150405"),
		fileBytes: cfg.fileBytes,
		highWater: cfg.diskHighWatermark,
		stdout:    cfg.stdout,
	}
	if err := s.open(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *fileSink) path(suffix string) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s-%06d%s", s.prefix, s.generation, suffix))
}

func (s *fileSink) open() error {
	f, err := os.OpenFile(s.path(partialSuffix), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o640)
	if err != nil {
		return fmt.Errorf("open sink file: %w", err)
	}
	s.f, s.written = f, 0
	return nil
}

func (s *fileSink) finish() error {
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	if s.written == 0 {
		return os.Remove(s.path(partialSuffix))
	}
	if rerr := os.Rename(s.path(partialSuffix), s.path(finalSuffix)); err == nil {
		err = rerr
	}
	return err
}

func (s *fileSink) gen() uint64 { return s.generation }

func (s *fileSink) maybeRotate() bool {
	if s.f != nil && s.written < s.fileBytes {
		return false
	}
	_ = s.finish()
	s.generation++
	if err := s.open(); err != nil {
		s.f = nil
	}
	return true
}

func (s *fileSink) write(line []byte) error {
	if s.f == nil {
		return errors.New("sink file is not open")
	}
	n, err := s.f.Write(line)
	s.written += int64(n)
	if err != nil {
		return err
	}
	if s.stdout {
		_, _ = os.Stdout.Write(line)
	}
	return nil
}

func (s *fileSink) diskFull() bool {
	s.sinceCheck++
	if s.sinceCheck < diskCheckEvery && !s.full {
		return false
	}
	s.sinceCheck = 0
	var total int64
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return s.full
	}
	for _, e := range entries {
		if info, err := e.Info(); err == nil && !e.IsDir() {
			total += info.Size()
		}
	}
	s.full = total >= s.highWater
	return s.full
}

func (s *fileSink) close() error { return s.finish() }

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '-' || r == '.' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, s)
}

func jsonLine(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// sessionLRU keeps the full-block ids of the most recent written record per
// session within the current file generation, bounded by bytes.
type sessionLRU struct {
	maxBytes int64
	bytes    int64
	ll       *list.List
	items    map[string]*list.Element
}

type lruEntry struct {
	key string
	ids []uint64
}

func newSessionLRU(maxBytes int64) *sessionLRU {
	return &sessionLRU{maxBytes: maxBytes, ll: list.New(), items: map[string]*list.Element{}}
}

func entryBytes(e *lruEntry) int64 { return int64(len(e.ids))*8 + int64(len(e.key)) + 64 }

func (c *sessionLRU) get(key string) ([]uint64, bool) {
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*lruEntry).ids, true
}

func (c *sessionLRU) put(key string, ids []uint64) {
	c.remove(key)
	e := &lruEntry{key: key, ids: ids}
	if entryBytes(e) > c.maxBytes {
		return
	}
	c.items[key] = c.ll.PushFront(e)
	c.bytes += entryBytes(e)
	for c.bytes > c.maxBytes {
		c.remove(c.ll.Back().Value.(*lruEntry).key)
	}
}

func (c *sessionLRU) remove(key string) {
	if el, ok := c.items[key]; ok {
		c.bytes -= entryBytes(el.Value.(*lruEntry))
		c.ll.Remove(el)
		delete(c.items, key)
	}
}

func (c *sessionLRU) reset() {
	c.ll.Init()
	c.items = map[string]*list.Element{}
	c.bytes = 0
}
