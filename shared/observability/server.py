"""A bounded background Prometheus HTTP server with graceful shutdown."""

import json
from collections.abc import Callable, Iterable
from threading import Thread
from typing import cast
from wsgiref.simple_server import WSGIServer, make_server
from wsgiref.types import StartResponse, WSGIEnvironment

from prometheus_client import CollectorRegistry, make_wsgi_app

from shared.observability.config import MetricsConfig


class MetricsServer:
    """Serve one isolated registry without blocking an application loop."""

    def __init__(
        self,
        config: MetricsConfig,
        registry: CollectorRegistry,
        health_check: Callable[[], bool] | None = None,
    ) -> None:
        self.config = config
        self.registry = registry
        self.health_check = health_check
        self._server: WSGIServer | None = None
        self._thread: Thread | None = None

    @property
    def active(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if not self.config.enabled or self.active:
            return
        metrics_app = make_wsgi_app(self.registry)

        def application(
            environ: WSGIEnvironment, start_response: StartResponse
        ) -> Iterable[bytes]:
            path = environ.get("PATH_INFO")
            if path == self.config.path:
                return cast(Iterable[bytes], metrics_app(environ, start_response))
            if path == "/health" and self.health_check is not None:
                healthy = self.health_check()
                body = json.dumps(
                    {"status": "healthy" if healthy else "unhealthy"}
                ).encode()
                start_response(
                    "200 OK" if healthy else "503 Service Unavailable",
                    [
                        ("Content-Type", "application/json"),
                        ("Content-Length", str(len(body))),
                    ],
                )
                return [body]
            start_response("404 Not Found", [("Content-Type", "text/plain")])
            return [b"not found\n"]

        self._server = make_server(self.config.host, self.config.port, application)
        self._thread = Thread(
            target=self._server.serve_forever,
            name=f"{self.config.service_name}-metrics",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._server = None
        self._thread = None
