# The event processor (Go port of the retired Python processor): same env,
# topics, tables, keys, metrics port and health file as before.
# confluent-kafka-go links its bundled librdkafka (glibc), so CGO stays on.
FROM golang:1.26-bookworm AS builder

WORKDIR /src
COPY services/event_processor_go/go.mod services/event_processor_go/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY services/event_processor_go/ ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/event-processor ./cmd/event-processor

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system processor \
    && useradd --system --gid processor --create-home processor

COPY --from=builder /out/event-processor /usr/local/bin/event-processor

USER processor

HEALTHCHECK --interval=20s --timeout=3s --start-period=15s --retries=3 \
    CMD ["event-processor", "healthcheck"]

ENTRYPOINT ["event-processor"]
