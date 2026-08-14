# Protocol Buffers (HEL-002)

This directory is the **source of truth** for the Go ↔ Python RPC contract.

Do not hand-write duplicate request/response structs for control-plane ↔ worker communication. Design the `.proto` here, then generate stubs into `gen/`.

## Expected shape (design it yourself)

Unary service with at least:

- `Infer` — text in, prediction + confidence out
- `Health` — structured health, not Docker logs

Keep proto3, unary RPCs only for V1. No streaming.

## After HEL-002

Place something like:

```text
proto/helios/inference/v1/inference.proto
```

Regenerate with `scripts/generate-proto.sh` (fill that script in HEL-002).
