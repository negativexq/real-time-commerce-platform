FROM python:3.12.11-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN groupadd --system commerce && useradd --system --gid commerce --home /app commerce
WORKDIR /app
COPY pyproject.toml README.md ./
COPY services ./services
COPY shared ./shared
RUN pip install --no-cache-dir .
USER commerce
EXPOSE 8080
CMD ["uvicorn", "services.demo_control_api.main:app", "--host", "0.0.0.0", "--port", "8080"]
