FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml README.md /app/
COPY shared /app/shared
COPY services /app/services
RUN pip install --no-cache-dir .
RUN useradd --create-home --uid 10001 appuser
USER appuser

CMD ["python", "-m", "services.fraud_outbox_publisher.main"]
