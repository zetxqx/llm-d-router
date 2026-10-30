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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/blake2s"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/prefixhash"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/record"
)

// PRF domain separators.
const (
	domainSession  = "sid"
	domainBlocks   = "blk"
	domainRequest  = "rid"
	domainReplica  = "rep"
	domainEndpoint = "ep"
	domainModel    = "model"
)

const (
	sampleBuckets = 10000
	flushBudget   = 20 * time.Second
)

// counters are process-local and exposed through DumpState.
type counters struct {
	written, droppedQueue, droppedSink, droppedDisk, keyframes, panics atomic.Int64
}

func (c *counters) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]int64{
		"written": c.written.Load(), "droppedQueue": c.droppedQueue.Load(), "droppedSink": c.droppedSink.Load(),
		"droppedDisk": c.droppedDisk.Load(), "keyframes": c.keyframes.Load(), "panics": c.panics.Load(),
	})
}

// job is the end-of-stream snapshot handed to the worker.
type job struct {
	st    *requestState
	resp  requestcontrol.Response
	bytes int64
}

// worker owns the sink and the delta state; only its goroutine touches them.
type worker struct {
	cfg        settings
	key        [blake2s.Size]byte
	rep        string
	sink       *fileSink
	lru        *sessionLRU
	stats      *counters
	queue      chan *job
	queueBytes atomic.Int64
	backendFn  func() string
	backend    string
	ctx        context.Context
}

func newWorker(ctx context.Context, cfg settings, rawKey []byte, sink *fileSink, stats *counters, backend func() string) *worker {
	w := &worker{
		cfg:       cfg,
		key:       blake2s.Sum256(rawKey), // normalizes any key length to the PRF key size
		sink:      sink,
		lru:       newSessionLRU(lruBytes),
		stats:     stats,
		queue:     make(chan *job, 4096),
		backendFn: backend,
		ctx:       ctx,
	}
	w.rep = w.prfHex(domainReplica, podName())
	go w.run()
	return w
}

// sampled decides per session (per request when sessionless) so that every
// replica holding the same key makes the same choice.
func (w *worker) sampled(sessionID, requestID string) bool {
	id, domain := sessionID, domainSession
	if id == "" {
		id, domain = requestID, domainRequest
	}
	return float64(w.prf64(domain, id)%sampleBuckets) < w.cfg.sampleRate*sampleBuckets
}

// enqueue never blocks; it drops when the byte budget or the queue is full.
// Later deltas stay decodable because the LRU only holds written records.
func (w *worker) enqueue(st *requestState, resp *requestcontrol.Response) {
	j := &job{st: st, resp: *resp, bytes: 512}
	if tr := st.request.Body; tr != nil && tr.TokenizedRequest != nil {
		j.bytes += int64(tr.TokenizedRequest.TokenCount()) * 4
	}
	if w.queueBytes.Add(j.bytes) <= queueBytes {
		select {
		case w.queue <- j:
			return
		default:
		}
	}
	w.queueBytes.Add(-j.bytes)
	w.stats.droppedQueue.Add(1)
}

func (w *worker) run() {
	defer func() { _ = w.sink.close() }()
	for {
		select {
		case j := <-w.queue:
			w.process(j)
		case <-w.ctx.Done():
			for deadline := time.Now().Add(flushBudget); time.Now().Before(deadline); {
				select {
				case j := <-w.queue:
					w.process(j)
				default:
					return
				}
			}
			return
		}
	}
}

func (w *worker) process(j *job) {
	defer w.queueBytes.Add(-j.bytes)
	defer func() {
		if v := recover(); v != nil {
			w.stats.panics.Add(1)
			log.FromContext(w.ctx).Error(fmt.Errorf("%v", v), "Recorder worker panicked")
		}
	}()
	if w.backend == "" {
		w.backend = w.backendFn()
	}
	if w.sink.maybeRotate() {
		w.lru.reset()
	}
	if w.sink.diskFull() {
		w.stats.droppedDisk.Add(1)
		return
	}
	rec, sessKey, fullIDs := w.encode(j)
	line, err := json.Marshal(rec)
	if err == nil {
		err = w.sink.write(append(line, '\n'))
	}
	if err != nil {
		w.stats.droppedSink.Add(1)
		w.lru.reset()
		log.FromContext(w.ctx).V(logutil.DEBUG).Info("Recorder sink write failed", "err", err.Error())
		return
	}
	w.stats.written.Add(1)
	if rec.Kf {
		w.stats.keyframes.Add(1)
	}
	if sessKey != "" && len(fullIDs) > 0 {
		w.lru.put(sessKey, fullIDs)
	}
}

// encode builds the record and returns the LRU key and the full-block ids to
// commit once the record is written.
func (w *worker) encode(j *job) (*record.Record, string, []uint64) {
	st, req, resp := j.st, j.st.request, &j.resp
	rec := &record.Record{
		V:          record.Version,
		Kid:        w.cfg.keyID,
		Rid:        w.prfHex(domainRequest, req.RequestID),
		Rep:        w.rep,
		Gen:        w.sink.generation,
		Model:      req.TargetModel,
		TokBackend: w.backend,
		PromptTok:  resp.Usage.PromptTokens,
		Events:     resp.StreamedEvents,
		Cause:      string(resp.TerminationCause),
		Profile:    st.profile,
		UsageSrc:   record.UsageSrcNone,
	}
	if !w.cfg.modelAllowlist[req.TargetModel] {
		rec.Model = "model-" + w.prfHex(domainModel, req.TargetModel)
	}
	if st.endpoint != "" {
		rec.Ep = w.prfHex(domainEndpoint, st.endpoint)
	}
	if req.Body != nil {
		rec.Stream, rec.MaxTokens = req.Body.Stream, req.Body.MaxOutputTokens
	}
	rec.Status, _ = strconv.Atoi(resp.Headers[":status"])
	if d := resp.Usage.PromptTokenDetails; d != nil {
		rec.CachedTok = ptr(d.CachedTokens)
	}
	switch {
	case resp.Usage.CompletionTokens > 0:
		rec.OutTok, rec.UsageSrc = resp.Usage.CompletionTokens, record.UsageSrcServer
	case resp.StreamedEvents > 0:
		rec.OutTok, rec.UsageSrc = resp.StreamedEvents, record.UsageSrcEvents
	}
	if t := resp.Timing; t != nil {
		rec.ArrMs = t.RequestReceived.UnixMilli()
		if res := w.cfg.arrivalResolutionMs; res > 0 {
			rec.ArrMs -= rec.ArrMs % res
		}
		rec.APIMs = t.Completed.Sub(t.RequestReceived).Milliseconds()
		if rec.Stream && !t.FirstByte.IsZero() {
			rec.TTFTMs = ptr(t.FirstByte.Sub(t.RequestReceived).Milliseconds())
		}
		if t.FlowControlAdmitted {
			rec.QueueMs = ptr(t.FlowQueueWait.Milliseconds())
		}
	}

	// Block ids are keyed per session so equal content in two sessions maps to
	// unrelated ids; sessionless requests get a per-request key.
	keyScope, sessKey := "r:"+req.RequestID, ""
	if st.sessionID != "" {
		sessKey = w.prfHex(domainSession, st.sessionID)
		rec.Sess, keyScope = &sessKey, "s:"+st.sessionID
	}
	ids, hashedTokens := w.blockIDs(j, keyScope)
	if rec.PromptTok == 0 {
		rec.PromptTok = hashedTokens
	}
	rec.Blocks = len(ids)
	fullIDs := ids[:hashedTokens/record.BlockSizeTokens]

	n := 0
	if prev, ok := w.lru.get(sessKey); ok {
		n = commonPrefix(prev, fullIDs)
	}
	if n == 0 {
		rec.Kf = true
	} else {
		rec.Anchor = &record.Anchor{N: n, H: record.HexID(ids[n-1])}
	}
	rec.New = record.EncodeIDs(ids[n:])
	return rec, sessKey, fullIDs
}

// blockIDs hashes the first prompt in 64-token blocks without a cap and maps
// each chain hash through a PRF keyed by keyScope.
func (w *worker) blockIDs(j *job, keyScope string) ([]uint64, int) {
	hashes, tokens := prefixhash.GetBlockHashesWithPromptTokens(w.ctx, j.st.request, record.BlockSizeTokens, math.MaxInt)
	if len(hashes) == 0 {
		return nil, 0
	}
	scoped := w.prf(domainBlocks, keyScope)
	ids := make([]uint64, len(hashes[0]))
	var in [8]byte
	for i, c := range hashes[0] {
		binary.LittleEndian.PutUint64(in[:], uint64(c))
		ids[i] = binary.LittleEndian.Uint64(mac(scoped, in[:]))
	}
	return ids, tokens[0]
}

// mac is keyed BLAKE2s-256; key is always 32 bytes, so New256 cannot fail.
func mac(key []byte, parts ...[]byte) []byte {
	h, _ := blake2s.New256(key)
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum(nil)
}

func (w *worker) prf(domain, value string) []byte {
	return mac(w.key[:], []byte(domain), []byte{0}, []byte(value))
}

func (w *worker) prf64(domain, value string) uint64 {
	return binary.LittleEndian.Uint64(w.prf(domain, value))
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

func ptr[T any](v T) *T { return &v }
