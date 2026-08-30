# syntax=docker/dockerfile:1.7
FROM golang:1.25.6-alpine AS build
WORKDIR /src/control-plane
COPY control-plane/go.mod control-plane/go.sum ./
RUN go mod download
COPY control-plane/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/apollo ./cmd/apollo \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/apollo /apollo
COPY --from=build /out/healthcheck /healthcheck
EXPOSE 50051 9090
HEALTHCHECK --interval=5s --timeout=2s --retries=5 CMD ["/healthcheck", "-address", "localhost:50051"]
ENTRYPOINT ["/apollo"]
