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

RUN groupadd --system generator \
    && useradd --system --gid generator --create-home generator

COPY --from=builder /wheels /wheels
RUN python -m pip install --no-cache-dir /wheels/* \
    && rm -rf /wheels

WORKDIR /app
COPY --chown=generator:generator scripts/generator-smoke.py ./scripts/generator-smoke.py

USER generator

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "from pathlib import Path; raise SystemExit(not Path('/tmp/event-generator-ready').exists())"]

ENTRYPOINT ["python", "-m", "services.event_generator.main"]
