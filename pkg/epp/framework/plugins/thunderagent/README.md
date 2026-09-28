# ThunderAgent Plugin

Session level admission control for agentic workloads, a minimal
implementation of ThunderAgent (arXiv 2602.13692). A session is an agent
trajectory identified by the request FairnessID (fill it with the
`agent-identity` plugin). No engine changes; clients only send a session id
header.

The problem: an agent session resends its whole history every turn, so it is
cheap only while its KV blocks survive on one pod. When a pod holds more
session state than its KV capacity, the engine evicts idle sessions' blocks
and every turn becomes a full prefill. The engine's own KV utilization
cannot detect this: blocks of a session waiting on a tool call sit on the
free list and are reported as unused.

## How it works

One named plugin instance fills every slot, so accounting and decisions
cannot drift apart:

| Slot | Role |
|---|---|
| scheduling profile (`Scorer`) | pin a session to the pod that holds its KV; abstain otherwise |
| `flowControl.saturationDetector` | refresh the per-pod ledger and run its maintenance each dispatch cycle; always reports unsaturated, because the controller's own gate would also block admitted sessions' turns |
| `defaultPriorityBand.fairnessPolicyRef` | the admission gate |
| request lifecycle (`PreRequest`, `ResponseBody`) | token accounting, pod binding |

Ledger: each session's KV footprint is the larger of the
`usage.total_tokens` of its last completed turn and the byte estimate of a
turn in flight (each turn resends the history, so the two describe the same
KV). Capacity is `block_size * num_gpu_blocks` scraped from the engine by
the datalayer metrics extractor (built in for vLLM, SGLang and other
engines). Only pods that report it enter the ledger; with none, the plugin
does nothing (the gate fails open and sessions are not pinned).

Gate: a pod's room is `utilThreshold * capacity` minus its working set.
Turns of admitted sessions always dispatch. A paused session's next turn
waits until its own pod has room (strict origin affinity: a session never
moves, so its warm prefix is never abandoned; the only exception is its pod
leaving the pool). A new session is admitted onto a pod with room, and holds
until one fits it. Among the heads that fit, reasoning goes before paused
before new; a head that does not fit does not block the others, so smaller
sessions can take room a larger paused one waits for. Any head waiting past
`headWaitStarvationMs` is force-admitted. A picked head's room is reserved
until its request reaches the pod.

Idle sessions give up room on demand, when a head is picked, longest idle
first. A paused or new session may take the room of sessions idle past
`idleLeaseSeconds`; a new session prefers a pod it fits without pausing
anyone. An admitted session's turn that pushes its pod over the ceiling may
take the room of any idle session. A session that gives up its room is
paused: it stops counting against the pod and its next turn must fit again.
Sessions with a turn queued are never paused. There is no periodic sweep:
pausing only matters to the next admission, so it happens there.

## Configuration

See `deploy/config/thunderagent-config.yaml` for the full pipeline. All
parameters:

| Field | Default | Description |
|---|---|---|
| `idleLeaseSeconds` | `30` | How long an idle session keeps its room against paused and new sessions. Set it to about a typical tool-call duration: shorter pauses sessions about to return, longer holds admissions behind long tool calls. |
| `utilThreshold` | `1.0` | Fit ceiling as a fraction of the pod's scraped KV capacity (1.0 = 100%). |
| `headWaitStarvationMs` | `1800000` | Forced-admission backstop; 0 disables it. |

Idle session state is dropped after an hour, or twice `headWaitStarvationMs`
if that is longer, so a held session is never dropped while it waits.

## Observability

Metrics (`thunder_agent_*`, all Alpha): `pod_working_set_tokens{pod}`
(compare it against the engine's KV utilization: a large working set over a
low utilization is the KV-thrashing signature),
`pod_capacity_tokens{pod}` (a pod without this series reports no capacity
and is not gated), `programs{state}`, `releases_total{class}`,
`holds_total{class}`, `pauses_total`, `starvation_promotions_total`. To verify the gate engaged during a run,
`pauses_total` and `holds_total` must be greater than zero. Session ids
never appear in metrics or logs.

## Limitations

- The ledger is in-process; running multiple EPP replicas splits the
  accounting.
- Anonymous traffic (no session header) passes the gate untracked; pair the
  plugin with a general load scorer to place it.
- Prefill/decode disaggregation is not supported: every pod the flow
  controller reports enters one ledger, so a new session can be reserved
  onto a prefill pod.
- There is no explicit end-of-session signal: a finished session stays in
  the working set as an idle session until another turn reclaims its room
  or the TTL drops it, so the working set gauge overestimates by recently
  ended sessions.
