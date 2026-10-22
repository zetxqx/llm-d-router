# Weka Trace Recorder

**Type:** `weka-trace-recorder`
**Interfaces:** `requestcontrol.PreRequest`, `requestcontrol.ResponseBodyProcessor`, `plugin.ConsumerPlugin`
**Stability:** Alpha (requires `--allow-experimental-plugins`)

Writes one content-free shape record per sampled request as JSONL. The offline
`weka-export` tool (`cmd/weka-export`) turns the records into WekaTrace v7 files
that aiperf loads directly. Design: [docs/proposals/weka-trace-recorder/design.md](../../../../../../docs/proposals/weka-trace-recorder/design.md).

## What a record contains

No prompt or completion text, no header values. Per request: a PRF of the
session id, arrival time, model (verbatim only if allowlisted), streaming flag,
prompt/cached/output token counts, 64-token block ids, TTFT, API time, flow
control queue time, termination cause, and PRFs of the endpoint and replica.

Block ids are a keyed PRF over the prefix chain hash with a key derived per
session, so equal content in two sessions produces unrelated ids. Records within
one file reference earlier records of the same session by an anchor (delta
encoding); the first record of a session in each file is a keyframe, so every
file decodes on its own.

## How it works

| Hook | Action |
|---|---|
| `PreRequest` | Resolves the session id from the agent-identity headers, decides sampling from a PRF of the session id (identical on every replica sharing the key), and stores a reference to the request. Always returns nil. |
| `ResponseBody` (end of stream only) | Reads `Usage`, `StreamedEvents`, `TerminationCause`, and the handler timestamps in `Response.Timing`, then enqueues a job without blocking. |
| Worker goroutine | Hashes the tokenized prompt in 64-token blocks without a cap, applies the PRF, delta-encodes against the session's last written record, and appends a line to the current file. |

Files are written as `<pod>-<start>-<gen>.jsonl.partial` and renamed to
`.jsonl` when closed. The recorder never deletes files; a collector uploads and
removes closed files. When the directory exceeds `diskHighWatermark`, new
records are dropped and counted.

The plugin consumes `TokenizedPrompt`, so a token producer is auto-created when
none is configured. Use the vLLM tokenizer backend for real token ids; the
`estimate` backend produces pseudo-tokens and the exporter refuses them by
default.

## Configuration

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `mode` | `dogfood` or `customer` | Yes | | `customer` requires `consent: true`, forbids stdout, and rounds arrival times to 100 ms. |
| `consent` | bool | For `customer` | `false` | |
| `keySecretPath` | string | Yes | | PRF key file. Missing or empty disables the recorder at runtime; requests are unaffected. |
| `keyID` | string | No | `""` | Key epoch label written into every record. |
| `sampleRate` | float | No | `0.01` | Fraction of sessions recorded. |
| `additionalSessionHeaders` | []string | No | `[]` | Checked before the agent-identity defaults. |
| `modelAllowlist` | []string | No | `[]` | Model names written verbatim; others become `model-<prf>`. |
| `sink.path` | string | No | `/var/weka-trace` | Output directory. |
| `sink.fileBytes` | quantity | No | `64Mi` | File size before starting a new file. |
| `sink.diskHighWatermark` | quantity | No | `900Mi` | Directory size above which new records are dropped. |
| `sink.stdout` | bool | No | `false` | Also write records to stdout (dogfood only). |
| `queueMaxBytes` | quantity | No | `64Mi` | Pending work budget, including referenced token slices. |
| `lruMaxBytes` | quantity | No | `64Mi` | Per-session delta state budget. |

```yaml
apiVersion: llm-d.ai/v1
kind: EndpointPickerConfig
plugins:
  - type: weka-trace-recorder
    parameters:
      mode: dogfood
      keySecretPath: /etc/weka-trace/key
      keyID: "2026-10"
      sampleRate: 0.1
```

Counters (written, dropped by reason, keyframes, panics, queue bytes) are
exposed through the plugin's `DumpState`.
