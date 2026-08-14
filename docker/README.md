# Docker (HEL-014, HEL-015)

| File                         | Ticket   | Purpose                          |
|------------------------------|----------|----------------------------------|
| controlplane.Dockerfile      | HEL-014  | Multi-stage Go image             |
| worker.Dockerfile            | HEL-014  | Python worker image              |
| docker-compose.yml           | HEL-015  | Control plane + N workers        |

Do not implement these until the corresponding tickets. Compose should use **service DNS names** for worker addresses (`worker-1:50051`), not `localhost`.
