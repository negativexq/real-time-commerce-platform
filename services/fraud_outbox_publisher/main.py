"""Fraud outbox process entry point."""

import signal

from services.fraud_outbox_publisher.config import OutboxConfig
from services.fraud_outbox_publisher.service import OutboxService


def main() -> int:
    service = OutboxService(OutboxConfig.from_environment())
    signal.signal(signal.SIGTERM, lambda signum, frame: service.stop())
    signal.signal(signal.SIGINT, lambda signum, frame: service.stop())
    try:
        service.run()
    finally:
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
