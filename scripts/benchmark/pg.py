"""Postgres helper for benchmark queries (host-side connection)."""

from collections.abc import Iterable
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row


@contextmanager
def connect(dsn: str):  # type: ignore[no-untyped-def]
    with psycopg.connect(dsn, row_factory=dict_row, autocommit=True) as conn:
        yield conn


def query_all(dsn: str, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())


def query_one(dsn: str, sql: str, params: Iterable[Any] = ()) -> dict[str, Any] | None:
    rows = query_all(dsn, sql, params)
    return rows[0] if rows else None
