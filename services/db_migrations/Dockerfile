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

RUN groupadd --system migrate \
    && useradd --system --gid migrate --create-home migrate

COPY --from=builder /wheels /wheels
RUN python -m pip install --no-cache-dir /wheels/* \
    && rm -rf /wheels

WORKDIR /app
COPY --chown=migrate:migrate database/migrations ./database/migrations

USER migrate

ENTRYPOINT ["python", "-m", "services.db_migrations"]
CMD ["migrate"]
