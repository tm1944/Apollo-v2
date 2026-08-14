# Python inference worker

Identical program/image for every replica. The Go control plane chooses which instance gets each request.

| Ticket   | What to put here                          |
|----------|-------------------------------------------|
| HEL-003  | gRPC server, stub `Infer` + `Health`      |
| HEL-005  | Load a small pretrained model at startup  |
| HEL-014  | Docker image uses this package            |

## Layout

```text
worker/
  requirements.txt     Python deps (add in HEL-003 / HEL-005)
  helios_worker/       Worker package
    __init__.py
    server.py          gRPC server + servicer
    model.py           Model load + infer (HEL-005)
```

## Run (after HEL-003)

```bash
python -m helios_worker.server
```

Use a virtualenv. Do not add PyTorch until HEL-005.
