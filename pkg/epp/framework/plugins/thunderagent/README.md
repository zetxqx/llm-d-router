# ThunderAgent Plugin

Program-aware admission control for agentic workloads, a port of the upstream ThunderAgent router's `tr` mode with acting-token decay (arXiv 2602.13692), consolidated into one plugin. A program is an agent trajectory identified by the request `FairnessID` (filled by the `agent-identity` plugin from session headers). The plugin tracks each program's KV token footprint in a single shared table and hooks that table into every layer that needs it:

| Slot | Interface | Behavior |
|---|---|---|
| Scheduling profile | `Scorer` | Reserved pod 1.0 for a program just admitted; sticky 1.0 for a bound program's pod; least decayed token load for unbound programs; abstains for anonymous traffic. |
| `flowControl.saturationDetector` | `SaturationDetector` | Maintains the per-pod fit view (two views of the working set against real scraped KV capacity), runs the pause sweep, and always reports unsaturated; the gate lives in the fairness policy. |
| `defaultPriorityBand.fairnessPolicyRef` | `FairnessPolicy` | The admission gate: turns of admitted programs always dispatch; paused and new programs dispatch only when they fit a pod, paused first, smallest first, with the forced-admission backstop. |
| Request lifecycle | `PreRequest` / `ResponseBodyProcessor` | Token accounting (usage totals plus in-flight estimates, updated while streaming), pod binding, resume, pause-mark maturation, session-final release. |

One named instance is referenced from all slots, so the accounting, the binding, and the admission decisions cannot drift apart.

## Why not engine-reported KV utilization

`vllm:kv_cache_usage_perc` counts only the blocks held by requests the engine is currently running. A program between turns (waiting on a tool) still owns its context in the prefix cache, but those blocks sit on the free list and are reported as unused. On an agentic workload most programs are idle at any instant, so engine-reported utilization stays near zero while the cache is in fact full. The committed-token footprint tracked here counts idle programs, which is the quantity that determines whether the next program fits.

## Accounting

- A program's footprint is the larger of the `usage.total_tokens` of its most recent completed request and the bytes-per-token estimate of a request still in flight. Values replace rather than accumulate, and in-flight and committed tokens are never summed: each agentic turn resends its whole history, so a turn's prefill covers the previous context and the two numbers describe the same KV. While a turn streams, the in-flight amount grows with the streamed events every 20 events (upstream `update_program_tokens_streaming`). The estimator is refined with momentum from every usage report.
- Per-pod capacity is `block_size * num_gpu_blocks` scraped from the engine's `cache_config_info` by the datalayer metrics extractor, handling heterogeneous pools. When absent, the configured `capacityTokens` is used, unless `requireRealCapacity` is set, in which case the pod is treated as unable to admit.
- Two views of a pod's working set are kept, as upstream does (`remaining_capacity` and `remaining_capacity_with_decay`). The undecayed view counts every unpaused program at full footprint and drives the pause sweep. The decayed view decays a program with no request in flight by `actingHalfLifeSeconds` (upstream: `2^-t`, t in seconds) and drives admission. Paused programs count in neither.
- Program state is released by the session-final header, or by the idle TTL sweep.

## Admission

The flow controller's saturation gate is a band-level head-of-line block: while a detector reports saturated, nothing dispatches, including the turns of programs already admitted. Those turns are what complete trajectories and free capacity, so this plugin never gates there. The `Saturation` hook always returns 0.0 and instead refreshes the per-pod fit view and runs the pause sweep.

**Pause sweep** (upstream `_pause_until_safe`, per pod, every `pauseSweepSeconds`): while the undecayed working set plus `bufferTokensPerProgram` per program exceeds `utilThreshold * capacity`, pause the smallest idle program (no request in flight, regardless of how long it has been idle). When no idle program is left, mark every in-flight program on the pod; a mark matures into a pause when that program's turn completes. Upstream's loop condition does not account for marks, so it marks them all, and so does this port. A paused program keeps its pod as its origin but stops counting against it.

**Gate** (upstream `_greedy_resume` plus the unchecked path for active programs), in the fairness policy where an admitted program's turn can be told apart from a paused or new program's turn:

- A REASONING program (bound, not paused, dispatched at least once) always dispatches: smallest footprint first, oldest head as the tiebreaker.
- A PAUSED program's next turn is fit-checked with the larger of its committed tokens and the new turn's estimate (upstream re-estimates from the request body before the program waits), plus the buffer, against the decayed room of a pod. It prefers its origin pod when that fits, otherwise the pod with the most room (`resumePlacement: most-room`, the default). With `resumePlacement: origin-only` it waits for its origin pod instead of moving, unless the origin has left the fit view (then it is placed by room) or the forced-admission backstop fires (then the scorer's sticky branch sends it to the origin regardless of room); the resumes that this policy delayed while another pod had room are counted in `thunder_agent_origin_waits_total`. Paused programs outrank new ones (upstream's REASONING group precedes NEW), smallest first; a paused program that fits nowhere never blocks a fitting newcomer (upstream skips non-fitting candidates too).
- A NEW program dispatches only when a pod has decayed room for `request bytes / bytes-per-token + bufferTokensPerProgram`, counting live admission reservations.
- An admitted paused or new program reserves that room on the pod that fit it until `PreRequest` binds it, and the scorer routes it to that pod, so back-to-back dispatch cycles cannot over-admit and the pod admitted onto is the pod picked.
- A paused or new head waiting past `urgentWaitMs` (off by default) is urgent: it outranks every non-urgent paused or new program and urgent heads go oldest first (proposal Part 1 aging). With `urgentMove` an urgent paused program may also leave its origin pod for the pod with the most room under `origin-only` (Part 3 Option B; measured harmful at 15 s on a saturated pool, see the verification repo). With `urgentReserveOrigin` the pod an urgent paused program is waiting for admits no other paused or new program until it fits, so freed room accumulates for the oldest waiter; turns of running programs are unaffected. Urgent programs still need a pod with room. `thunder_agent_urgent_promotions_total` counts urgent dispatches, `thunder_agent_reserved_pods` the pods currently reserved.
- Any head waiting past `headWaitStarvationMs` dispatches regardless of class, size, or fit, oldest first: the forced-admission backstop (upstream `_wait_for_resume`, 1800 s).

When only non-fitting paused or new programs wait, the policy picks nothing and the requests stay queued; that is the hold.

There is no ACTING class. Upstream proactively pauses idle programs, so its resume pool can hold a program with no request outstanding. Flow control here only ever queues actual requests, so every candidate has a request pending.

## Session lifecycle

A request carrying the session-final header (`x-session-final: true` by default) marks its program; when that turn's response completes (and no other turn of the program is in flight), the program's accounting and binding are dropped immediately instead of waiting for the idle TTL. A paused program's final turn resumes it for the turn and then releases it. A standalone release request (final header on an empty prompt) works as a tiny turn. `x-parent-session-id` is recorded on subagent programs for observability only.

## Configuration

```yaml
- type: thunder-agent
  name: thunder
  parameters:
    capacityTokens: 524288
    utilThreshold: 1.0
    actingHalfLifeSeconds: 1
    bufferTokensPerProgram: 100
    pauseSweepSeconds: 5
    resumePlacement: most-room
    urgentWaitMs: 0
    headWaitStarvationMs: 1800000
    evictionTtlSeconds: 3600
    evictionSweepSeconds: 300
    sessionFinalHeader: x-session-final
```

| Field | Default | Upstream equivalent | Description |
|---|---|---|---|
| `capacityTokens` | `4194304` | fetched cache config | Per-pod KV capacity in tokens when `cache_config_info` is not scraped. |
| `requireRealCapacity` | `false` | - | Treat a pod without scraped capacity as having no admission room instead of using `capacityTokens`. |
| `utilThreshold` | `1.0` | none (full capacity) | Fit ceiling as a fraction of capacity, for both the sweep and admission. |
| `actingHalfLifeSeconds` | `1` | `2^-t`, fixed | Half-life of an idle program's footprint in the admission view. `0` disables decay. |
| `bufferTokensPerProgram` | `100` | `BUFFER_PER_PROGRAM = 100` | Reserved per program in both views and in the admission fit check. |
| `pauseSweepSeconds` | `5` | `scheduler_interval = 5` | Interval of the per-pod pause sweep. `0` sweeps on every dispatch cycle. |
| `kvUsageCorrection` | `false` | `shared_tokens` (never activated upstream) | Subtract, per pod, the running programs' estimate minus the engine's reported KV usage. Needs real scraped capacity and metrics. |
| `resumePlacement` | `most-room` | BFD re-placement (`most-room`) | Where a paused program's next turn may go. `most-room`: origin pod if it fits, else the pod with the most room. `origin-only`: wait for the origin pod; move only when it left the pool or the backstop fires. New programs always go to the pod with the most room. |
| `urgentWaitMs` | `0` (off) | none | Urgent tier: a paused or new head that waited this long is ordered ahead of all non-urgent paused and new heads (oldest first). Fit-checked. Must be below `headWaitStarvationMs`. |
| `urgentMove` | `false` | none | Under `origin-only`, let an urgent paused program take any pod with room instead of waiting for its origin. Needs `urgentWaitMs`. |
| `urgentReserveOrigin` | `false` | none | While an urgent paused program does not fit its origin pod, admit no other paused or new program onto that pod. Needs `urgentWaitMs`. |
| `headWaitStarvationMs` | `1800000` | `_wait_for_resume` timeout 1800 s | Forced admission of any head waiting this long. `0` disables it. `30000` is the improved variant. |
| `evictionTtlSeconds` | `3600` | none (upstream leaks unreleased programs) | A program with no in-flight request and no activity in this window is dropped. Must exceed `headWaitStarvationMs`. |
| `evictionSweepSeconds` | `300` | - | How often the eviction sweep runs. |
| `sessionFinalHeader` | `x-session-final` | `POST /programs/release` | Header marking a session's last turn. |
| `parentSessionHeader` | `x-parent-session-id` | - | Header carrying a subagent's parent session ID. |
| `profileName` | primary profile | - | Scheduling profile whose picked endpoint a program is attributed to. |

See `deploy/config/thunderagent-config.yaml` for the full pipeline, including `flowControl.defaultRequestTTL: "0s"` (upstream never rejects a held request) and the `agent-identity` plugin that fills `FairnessID`.

## Deviations from upstream ThunderAgent

- **Resume placement.** Upstream re-places a resumed program on the backend with the most free capacity (best-fit-decreasing), which can move it off its warm prefix cache. Here a paused program resumes onto its origin pod whenever that fits, otherwise onto the pod with the most room; `resumePlacement: origin-only` goes further and waits for the origin pod, which upstream never does.
- **Event-driven admission.** Upstream admits paused and new programs on its 5 s tick, and a new program is admitted directly against the undecayed view only when nothing waits. Here admission is re-evaluated on every dispatch cycle against the decayed view, so a hold ends as soon as room exists and admission is per pod rather than a pool-wide cumulative selection.
- **Overlapping turns.** A pause mark matures when the program has no turn in flight; upstream pauses on the first completed response.
- **Urgent tier.** `urgentWaitMs` reorders waiting programs by age once they have waited that long; `urgentMove` frees them from origin-only placement; `urgentReserveOrigin` holds a pod for them. Upstream orders the resume pool by size only and has no deadline notion.
- **Forced admission and TTL knobs.** `headWaitStarvationMs` below 1800 s and `pauseSweepSeconds: 0` are improvements available for an A/B, not upstream behavior. Flow control's request TTL can reject held requests with 429 if set; the shipped config disables it.
- **Vanished pods.** A bound program whose pod left the pool is invisible to the sweep until its next turn rebinds it; a paused program whose origin left is placed by room.
- **Single-replica accounting.** The program table is in-process. Running multiple EPP replicas splits the accounting, the same limitation as upstream's single router instance.

## Observability

Metrics (`thunder_agent_*`, all Alpha): per-pod `pod_utilization` (undecayed working set over capacity) and `pod_capacity_tokens{source="real|fallback"}` gauges, a `programs{state="running|idle|marked|paused"}` gauge, and counters `holds_total{class}`, `releases_total{class}` (classes `reasoning`, `paused`, `new`), `starvation_promotions_total`, `rebinds_total`, `pauses_total`, `resumes_total`, `session_final_releases_total`, `ttl_evictions_total`.

To verify the plugin engaged during a run: `thunder_agent_pauses_total` and `thunder_agent_holds_total` must be greater than zero and `thunder_agent_pod_utilization` must have reached `utilThreshold`. `releases_total{class="paused"}` under pressure confirms the resume path drove dispatch. The generic flow control metrics (`flow_control_queue_size`, `flow_control_request_queue_duration_seconds`) witness holds independently; `flow_control_pool_saturation` reads 0 by design with this plugin as the detector.

The state dump endpoint reports sanitized aggregates (program counts, paused count, per-pod undecayed and decayed tokens, pause and resume totals, estimator ratio). Program IDs come from a user-controlled header and are never exported in metrics, logs, or dumps.
