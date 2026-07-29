"""Fraud outbox process entry point."""

import signal

from services.fraud_outbox_publisher.config import OutboxConfig
from services.fraud_outbox_publisher.service import OutboxService
from shared.observability import ApplicationMetrics, MetricsServer


def main() -> int:
    config = OutboxConfig.from_environment()
    metrics = ApplicationMetrics(config.metrics.service_name, config.metrics.namespace)
    server = MetricsServer(config.metrics, metrics.registry)
    try:
        server.start()
    except OSError:
        metrics.outbox_healthy.set(0)
    service = OutboxService(config, metrics)
    signal.signal(signal.SIGTERM, lambda signum, frame: service.stop())
    signal.signal(signal.SIGINT, lambda signum, frame: service.stop())
    try:
        service.run()
    finally:
        service.close()
        server.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
