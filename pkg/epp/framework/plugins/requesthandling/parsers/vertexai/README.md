# Vertex AI Parser Plugin

**Type:** `vertexai-parser`

Parses H2C (HTTP/2 cleartext) requests and responses in the Vertex AI gRPC API format. This parser is typically used when the EPP fronts a backend running the Vertex AI PredictionService.

Extracts raw JSON payloads embedded within Vertex AI's gRPC request/response framing (specifically under `HttpBody`) and delegates the extraction to the OpenAI parser.

Supports the following Vertex AI `PredictionService` endpoints:

- `PredictionService/ChatCompletions` (maps to standard `/chat/completions` OpenAI payloads)
- `PredictionService/StreamRawPredict` (forced to use the OpenAI response API format, mapping to standard `/responses` OpenAI payloads)

---

## Protocol & API Reference

The gRPC services and payloads parsed by this plugin are defined in Google's official API definitions:

- [Vertex AI PredictionService Proto Definition](https://github.com/googleapis/googleapis/blob/89c3153888201c9e80bc5ec78d6ffca0debe6b52/google/cloud/aiplatform/v1beta1/prediction_service.proto)

---

## Unsupported Endpoints

If an incoming gRPC request path does not match any of the supported Vertex AI endpoints:

- The parser returns `&fwkrh.ParseResult{Skip: true}, nil`.
- This instructs the EPP requesthandling framework to skip this parser and try the next registered parser in the configuration chain.

---

## Adding a New Endpoint

To add support for a new Vertex AI gRPC endpoint:

1. **Define the Path Suffix**: In `vertexai.go`, declare the suffix constant for the new gRPC prediction endpoint (e.g., `PredictionService/NewMethod`) and its target OpenAI-style mapping path.
2. **Update `ParseRequest`**: Add a new matched case in the `ParseRequest` switch block.
3. **Update `vertexai_test.go`**

---

## Related Documentation

- [Parsers Index](../README.md)
