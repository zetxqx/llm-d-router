# Session State Producer

The `session-state-producer` tracks the state of sessions identified by the
`agent-identity` request-header plugin in a `SessionStateRegistry`, and
publishes it on request and endpoint attributes for flow-control, admission
and scheduling plugins.

The producer uses only the `agent-identity` attribute. `SessionIDDataKey` is a
separate attribute produced by `session-id-producer` from one configured header
or cookie for general session-affinity use.

## Configuration

Both plugins must be enabled, and Alpha plugins must be allowed by the EPP
process. `agent-identity` is a required data dependency, so configuration
loading fails if no plugin declares that it produces the agent identity
attribute. A plugin that requires `SessionStateDataKey`, such as
`thunder-agent`, creates a `session-state-producer` with default parameters
when none is configured.

```yaml
plugins:
- type: agent-identity
- type: session-state-producer
  parameters:
    evictionTtlSeconds: 3600
    evictionSweepSeconds: 300
```

| Parameter | Default | Description |
|---|---:|---|
| `evictionTtlSeconds` | `3600` | Maximum session idle time before its state is removed. Set to `0` to disable eviction. Keep it above the longest time a request can wait in flow control, or a waiting session loses its state. |
| `evictionSweepSeconds` | `300` | Interval between idle-state scans. Must be greater than `0`. |

## Hooks

| Hook | Effect |
|---|---|
| `RequestHeader` | Marks the session as seen, creating it as `new` on first sight, and attaches a dynamic `SessionStateDataKey` attribute. This runs before flow control. |
| `PreRequest` | Records one dispatched turn on the primary profile's endpoint: binds the session to it, adds the turn's prompt tokens to `InFlightTokens`, and sets `running`. Multiple scheduling profiles count as one turn. |
| `ResponseBody` (end of stream) | Settles the turn: releases its `InFlightTokens`, replaces `CommittedTokens`, and moves the session to `acting` or `idle` when its last turn ends. |
| `Extract` (endpoint events) | Registers endpoints and their KV capacity, publishes `EndpointSessionDataKey`, and unbinds sessions from deleted endpoints. |

## Request attribute: `SessionStateDataKey`

The attribute is a `scheduling.DynamicRequestAttribute`: every read through
`sessionstate.ReadSessionState(request)` returns the current
`sessionstate.SessionState`, including while the request waits in a
flow-control queue and after other turns of the same session change it.

- `TurnsTaken`: turns dispatched.
- `Duration`: time since the session was first observed, computed on read.
- `LastSeenAt`: time of the latest request, dispatch or response activity.
- `InFlightRequests`: dispatched turns whose response lifecycle has not ended.
- `CompletedRequests`: turns whose responses ended naturally. A complete model
  error response is a natural completion.
- `TotalInputTokens`, `TotalOutputTokens`: prompt and completion tokens summed
  over naturally completed turns.
- `Status`: `new`, `running`, `acting`, `idle` or `archived` (see below).
- `BoundEndpoint`: the endpoint holding the session's KV cache, or empty.
- `LastBoundEndpoint`: the endpoint the session was most recently bound to,
  kept after it is unbound.
- `CommittedTokens`: total tokens of the latest finished turn. Each turn
  resends the conversation history, so the value is replaced, not summed. A
  turn without usage data falls back to its prompt token count.
- `InFlightTokens`: full prompt tokens of the turns in flight: the tokenized
  count when a token producer ran, else the request body size divided by 4.
- `LastStateChangeAt`, `LastResponseAt`, `LastFinishReason`,
  `LastTurnHadToolCall`.

A running session's KV footprint on its endpoint is
`max(CommittedTokens, InFlightTokens)` (`SessionState.WorkingSetTokens`).

| Status | Meaning | Entered when |
|---|---|---|
| `new` | First turn seen, not yet dispatched. | `RequestHeader` of an unknown session. |
| `running` | At least one turn in flight on `BoundEndpoint`. | `PreRequest`. |
| `acting` | No turn in flight; the latest turn ended in a tool call. | The last turn ends with `HasToolCalls`. |
| `idle` | No turn in flight; the latest turn ended without a tool call. | The last turn ends without `HasToolCalls`. |
| `archived` | The client or harness archived the session; its KV blocks may be released. | `SessionStateRegistry.Archive`. A session with a turn in flight is archived when its last turn ends. |

Client disconnects, evictions and internal errors do not contribute to
`CompletedRequests` or the token totals because their usage may be incomplete.

## Endpoint attribute: `EndpointSessionDataKey`

An `EndpointSessionSummary` per endpoint, read through
`sessionstate.ReadEndpointSessionSummary(endpoint.GetAttributes())`:
`CapacityTokens` (`CacheBlockSize * CacheNumBlocks`, 0 when not reported) and
the working set of the sessions bound to the endpoint by status
(`RunningTokens`, `ActingTokens`, `IdleTokens`, `ArchivedTokens`).

Plugins that do not receive endpoints, such as flow-control policies, query
the registry directly through `sessionstate.LookupProducer(handle)` and its `Registry()`:
`State`, `HasEndpoint`, `EndpointCapacities`, `EndpointSummary`,
`ForEachEndpoint` and `ForEachEndpointSession`. Only endpoints registered by
endpoint events are reported; a turn dispatched to an endpoint the registry
was never told about binds the session without making the endpoint visible.

## Eviction

- Idle sessions are removed after `evictionTtlSeconds` without activity.
  Sessions with in-flight requests are retained until those requests end.
- When the sessions bound to an endpoint with known capacity exceed it, the
  registry models the engine evicting KV blocks: it takes tokens from sessions
  with no turn in flight, archived first, then idle, then acting, each oldest
  first. A session whose `CommittedTokens` reaches 0 is unbound, and removed if
  archived.
- KV-event driven eviction is not implemented.

State is local to one EPP replica.
