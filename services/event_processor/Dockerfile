FROM python:3.12.11-slim-bookworm AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build

COPY pyproject.toml README.md ./
COPY shared ./shared
COPY services ./services

RUN python -m pip wheel --no-cache-dir --wheel-dir /wheels .

FROM python:3.12.11-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN groupadd --system processor \
    && useradd --system --gid processor --create-home processor

COPY --from=builder /wheels /wheels
RUN python -m pip install --no-cache-dir /wheels/* \
    && rm -rf /wheels

WORKDIR /app
COPY --chown=processor:processor scripts/processor-*.py ./scripts/
COPY --chown=processor:processor scripts/persistence-*.py ./scripts/
COPY --chown=processor:processor database/migrations ./database/migrations

USER processor

HEALTHCHECK --interval=20s --timeout=3s --start-period=15s --retries=3 \
    CMD ["python", "-c", "from pathlib import Path; raise SystemExit(not Path('/tmp/event-processor-healthy').exists())"]

ENTRYPOINT ["python", "-m", "services.event_processor.main"]
