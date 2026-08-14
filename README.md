# Apollo v2

Distributed AI inference platform (V1 learning project).

Clients send HTTP requests to a **Go control plane**. The control plane schedules work onto identical **Python gRPC workers** that run a PyTorch model.

This repository is a **skeleton**. Fill in packages as you complete HEL tickets. Do not treat empty files as a finished design.

```text
Client
  │  POST /v1/inference
  ▼
Go Control Plane  ──gRPC──►  Python Worker(s) + PyTorch
```

## Layout

```text
cmd/controlplane/     Control plane process entrypoint (HEL-001, later HEL-013)
internal/config/      Flags and environment (HEL-001+)
internal/httpapi/     HTTP handlers: healthz, inference (HEL-001, HEL-006)
internal/worker/      Worker record types (HEL-007)
internal/registry/    In-memory worker registry (HEL-008)
internal/scheduler/   Least-loaded scheduling (HEL-009, HEL-010)
internal/grpcclient/  gRPC client to workers (HEL-004, HEL-012)
internal/healthcheck/ Background Health RPC loop (HEL-011)
internal/shutdown/    Graceful shutdown wiring (HEL-013)
proto/                Source .proto contract (HEL-002)
gen/                  Generated Go/Python stubs (HEL-002)
worker/               Python gRPC worker (HEL-003, HEL-005)
docker/               Dockerfiles + Compose (HEL-014, HEL-015)
scripts/              Codegen and smoke helpers (HEL-002, HEL-016)
```

`internal/` cannot be imported by other modules. That is intentional: the control plane is the product, not a library.

## Prerequisites

- Go 1.25+
- Python 3.11+ (3.14 is fine for later worker tickets)
- Docker (HEL-014+)

## Run (after HEL-001)

```bash
go run ./cmd/controlplane
```

Until you implement HEL-001, this process starts and exits immediately.

```bash
go test ./...
go test -race ./...
```

## Working agreement

Implement one HEL ticket at a time. See `.agents/apollo-v1-tickets.md` locally (gitignored). When a ticket is done, ask for review rather than jumping ahead.
