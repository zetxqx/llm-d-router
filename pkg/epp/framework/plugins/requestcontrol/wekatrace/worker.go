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
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"math"
	"strconv"
	"time"

	"golang.org/x/crypto/blake2s"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/prefixhash"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
)

// PRF domain separators.
const (
	domainSession = "sid"
	domainBlocks  = "blk"
	domainRequest = "rid"
	domainReplica = "rep"
	domainEndpt   = "ep"
	domainModel   = "model"
)

// sampleBuckets is the resolution of the sampling decision.
const sampleBuckets = 10000

// job is the end-of-stream snapshot handed to the worker.
type job struct {
	st       *requestState
	usage    usageSnapshot
	events   int
	cause    requestcontrol.TerminationCause
	timing   requestcontrol.ResponseTiming
	status   int
	bytes    int64
	hasTimer bool
}

type usageSnapshot struct {
	prompt     int
	completion int
	cached     *int
}

func newJob(st *requestState, resp *requestcontrol.Response) *job {
	j := &job{st: st, events: resp.StreamedEvents, cause: resp.TerminationCause}
	j.usage.prompt = resp.Usage.PromptTokens
	j.usage.completion = resp.Usage.CompletionTokens
	if d := resp.Usage.PromptTokenDetails; d != nil {
		c := d.CachedTokens
		j.usage.cached = &c
	}
	if resp.Timing != nil {
		j.timing = *resp.Timing
		j.hasTimer = true
	}
	if s, err := strconv.Atoi(resp.Headers[":status"]); err == nil {
		j.status = s
	}
	j.bytes = 512
	if body := st.request.Body; body != nil && body.TokenizedRequest != nil {
		j.bytes += int64(body.TokenizedRequest.TokenCount()) * 4
	}
	return j
}

// sink is where encoded records go. gen identifies the current file; write
// reports whether the record became durable in that file.
type sink interface {
	gen() uint64
	// maybeRotate starts a new file when the current one is full and reports
	// whether the generation changed.
	maybeRotate() bool
	write(line []byte) error
	diskFull() bool
	close() error
}

type worker struct {
	cfg          resolvedConfig
	key          []byte
	kid          string
	rep          string
	sink         sink
	stats        *stats
	queue        chan *job
	tokenBackend func() string
	backend      string
	lru          *sessionLRU
	ctx          context.Context
}

func newWorker(ctx context.Context, cfg resolvedConfig, key []byte, s sink, st *stats, tokenBackend func() string) *worker {
	w := &worker{
		cfg:          cfg,
		key:          key,
		kid:          cfg.keyID,
		sink:         s,
		stats:        st,
		queue:        make(chan *job, 4096),
		tokenBackend: tokenBackend,
		lru:          newSessionLRU(cfg.lruBytes),
		ctx:          ctx,
	}
	w.rep = w.prfHex(domainReplica, podName())
	go w.run()
	return w
}

// sampled decides per session (or per request when sessionless) so that every
// replica holding the same key makes the same choice.
func (w *worker) sampled(sessionID, requestID string) bool {
	if w.cfg.sampleRate >= 1 {
		return true
	}
	if w.cfg.sampleRate <= 0 {
		return false
	}
	id, domain := sessionID, domainSession
	if id == "" {
		id, domain = requestID, domainRequest
	}
	return w.prf64(domain, id)%sampleBuckets < uint64(w.cfg.sampleRate*sampleBuckets)
}

// enqueue never blocks the caller; it drops when the byte budget is exhausted.
func (w *worker) enqueue(j *job) {
	if w.stats.queueBytes.Add(j.bytes) > w.cfg.queueBytes {
		w.stats.queueBytes.Add(-j.bytes)
		w.dropQueue(j)
		return
	}
	select {
	case w.queue <- j:
	default:
		w.stats.queueBytes.Add(-j.bytes)
		w.dropQueue(j)
	}
}

// dropQueue only counts: the LRU holds the last written record, so later
// deltas stay decodable without the dropped one.
func (w *worker) dropQueue(_ *job) {
	w.stats.droppedQueue.Add(1)
}

func (w *worker) run() {
	defer func() { _ = w.sink.close() }()
	for {
		select {
		case <-w.ctx.Done():
			w.drain(time.Now().Add(20 * time.Second))
			return
		case j := <-w.queue:
			w.processSafe(j)
		}
	}
}

// drain writes what is already queued until deadline.
func (w *worker) drain(deadline time.Time) {
	for time.Now().Before(deadline) {
		select {
		case j := <-w.queue:
			w.processSafe(j)
		default:
			return
		}
	}
}

func (w *worker) processSafe(j *job) {
	defer w.stats.queueBytes.Add(-j.bytes)
	defer func() {
		if v := recover(); v != nil {
			w.stats.workerPanics.Add(1)
			log.FromContext(w.ctx).Error(fmt.Errorf("%v", v), "Recorder worker panicked")
		}
	}()
	w.process(j)
}

func (w *worker) process(j *job) {
	if w.backend == "" {
		w.backend = w.tokenBackend()
	}
	if w.sink.maybeRotate() {
		w.lru.reset()
	}
	if w.sink.diskFull() {
		w.stats.droppedDisk.Add(1)
		return
	}

	rec, sessKey, fullIDs := w.encode(j)
	line, err := jsonLine(rec)
	if err == nil {
		err = w.sink.write(line)
	}
	if err != nil {
		w.stats.droppedSink.Add(1)
		w.lru.reset()
		log.FromContext(w.ctx).V(logutil.DEBUG).Info("Recorder sink write failed", "err", err.Error())
		return
	}
	w.stats.written.Add(1)
	w.stats.bytesWritten.Add(int64(len(line)))
	if rec.Kf {
		w.stats.keyframes.Add(1)
	}
	if sessKey != "" && len(fullIDs) > 0 {
		w.lru.put(sessKey, fullIDs)
	}
}

// encode builds the record and returns the LRU key and full-block ids to
// commit after a successful write.
func (w *worker) encode(j *job) (*record.Record, string, []uint64) {
	st := j.st
	req := st.request
	rec := &record.Record{
		V:          record.Version,
		Kid:        w.kid,
		Rid:        w.prfHex(domainRequest, req.RequestID),
		Rep:        w.rep,
		Gen:        w.sink.gen(),
		Model:      w.model(req.TargetModel),
		TokBackend: w.backend,
		Cause:      string(j.cause),
		Status:     j.status,
		Profile:    st.profile,
		Events:     j.events,
	}
	if st.endpoint != "" {
		rec.Ep = w.prfHex(domainEndpt, st.endpoint)
	}
	if req.Body != nil {
		rec.Stream = req.Body.Stream
		rec.MaxTokens = req.Body.MaxOutputTokens
	}

	blkScope := "r:" + req.RequestID
	sessKey := ""
	if st.sessionID != "" {
		s := w.prfHex(domainSession, st.sessionID)
		rec.Sess = &s
		sessKey = s
		blkScope = "s:" + st.sessionID
	}

	w.fillTiming(rec, j)
	w.fillUsage(rec, j)

	ids, hashedTokens := w.blockIDs(req, blkScope)
	if rec.PromptTok == 0 {
		rec.PromptTok = hashedTokens
	}
	rec.Blocks = len(ids)
	fullIDs := ids[:hashedTokens/record.BlockSizeTokens]

	n := 0
	if sessKey != "" {
		if prev, ok := w.lru.get(sessKey); ok {
			n = commonPrefix(prev, fullIDs)
		}
	}
	if n == 0 {
		rec.Kf = true
		rec.New = record.EncodeIDs(ids)
	} else {
		rec.Anchor = &record.Anchor{N: n, H: record.HexID(ids[n-1])}
		rec.New = record.EncodeIDs(ids[n:])
	}
	return rec, sessKey, append([]uint64(nil), fullIDs...)
}

func (w *worker) fillTiming(rec *record.Record, j *job) {
	if !j.hasTimer {
		return
	}
	t := j.timing
	arr := t.RequestReceived.UnixMilli()
	if res := w.cfg.arrivalResolutionMs; res > 0 {
		arr = arr / res * res
	}
	rec.ArrMs = arr
	rec.APIMs = t.Completed.Sub(t.RequestReceived).Milliseconds()
	if rec.Stream && !t.FirstByte.IsZero() {
		v := t.FirstByte.Sub(t.RequestReceived).Milliseconds()
		rec.TTFTMs = &v
	}
	if t.FlowControlAdmitted {
		v := t.FlowQueueWait.Milliseconds()
		rec.QueueMs = &v
	}
}

func (w *worker) fillUsage(rec *record.Record, j *job) {
	rec.PromptTok = j.usage.prompt
	rec.CachedTok = j.usage.cached
	switch {
	case j.usage.completion > 0:
		rec.OutTok = j.usage.completion
		rec.UsageSrc = record.UsageSrcServer
	case j.events > 0:
		rec.OutTok = j.events
		rec.UsageSrc = record.UsageSrcEvents
	default:
		rec.UsageSrc = record.UsageSrcNone
	}
}

// blockIDs hashes the first prompt in 64-token blocks without a cap and maps
// each chain hash through a keyed PRF whose key is derived from keyScope, so
// ids are comparable only within one session.
func (w *worker) blockIDs(req *fwksched.InferenceRequest, keyScope string) ([]uint64, int) {
	hashes, tokens := prefixhash.GetBlockHashesWithPromptTokens(w.ctx, req, record.BlockSizeTokens, math.MaxInt)
	if len(hashes) == 0 || len(tokens) == 0 {
		return nil, 0
	}
	h := w.mac(w.prf(domainBlocks, keyScope))
	ids := make([]uint64, len(hashes[0]))
	var in [8]byte
	sum := make([]byte, 0, blake2s.Size)
	for i, c := range hashes[0] {
		h.Reset()
		binary.LittleEndian.PutUint64(in[:], uint64(c))
		h.Write(in[:])
		ids[i] = binary.LittleEndian.Uint64(h.Sum(sum[:0])[:8])
	}
	return ids, tokens[0]
}

func (w *worker) model(name string) string {
	if _, ok := w.cfg.modelAllowlist[name]; ok {
		return name
	}
	return "model-" + w.prfHex(domainModel, name)
}

func (w *worker) mac(key []byte) hash.Hash {
	h, err := blake2s.New256(key)
	if err != nil {
		// blake2s rejects keys longer than 32 bytes; hash such keys down first.
		sum := blake2s.Sum256(key)
		h, _ = blake2s.New256(sum[:])
	}
	return h
}

func (w *worker) prf(domain, value string) []byte {
	h := w.mac(w.key)
	h.Write([]byte(domain))
	h.Write([]byte{0})
	h.Write([]byte(value))
	return h.Sum(nil)
}

func (w *worker) prf64(domain, value string) uint64 {
	return binary.LittleEndian.Uint64(w.prf(domain, value)[:8])
}

func (w *worker) prfHex(domain, value string) string {
	return hex.EncodeToString(w.prf(domain, value)[:8])
}

func commonPrefix(a, b []uint64) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
