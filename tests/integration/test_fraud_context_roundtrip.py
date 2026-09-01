"""Semantic-equivalence and round-trip-count tests for the fraud-context
customer+order consolidation (see docs/performance/optimization-history.md,
"Fraud-context round-trip reduction").

These tests connect to a real, already-running local PostgreSQL instance
(the same one every benchmark/smoke script in this repo uses) - there is no
mocked cursor because the change being verified is a change to the SQL
itself. All fixture writes happen inside one transaction that is always
rolled back, so nothing is left behind regardless of outcome.

Requires the local Docker stack's PostgreSQL to be reachable; skipped
automatically if it is not.
"""

from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest

from scripts.benchmark.config import load_config
from services.event_processor.errors import FraudContextDependencyError
from services.event_processor.fraud.context import FraudContextBuilder

pytestmark = pytest.mark.integration


def _connect() -> psycopg.Connection[tuple[object, ...]] | None:
    dsn = load_config("fraud-context-roundtrip-test").postgres_dsn
    try:
        return psycopg.connect(dsn, autocommit=False, connect_timeout=2)
    except psycopg.OperationalError:
        return None


@pytest.fixture
def connection() -> object:
    conn = _connect()
    if conn is None:
        pytest.skip("local PostgreSQL is not reachable")
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


class Fixture:
    """Deterministic customer/order/session/cart rows for one test."""

    def __init__(self, cursor: psycopg.Cursor[tuple[object, ...]]) -> None:
        self.event_id = uuid4()
        self.customer_id = uuid4()
        self.other_customer_id = uuid4()
        self.session_id = uuid4()
        self.cart_id = uuid4()
        self.order_id = uuid4()
        self.order_of_other_customer_id = uuid4()
        self.missing_order_id = uuid4()
        now = datetime.now(UTC)

        cursor.execute(
            """
            INSERT INTO processed_events
                (event_id, event_type, event_version, event_time,
                 produced_at, source, correlation_id, payload_json)
            VALUES (%s, 'order_created', 1, %s, %s, 'test', %s, '{}')
            """,
            (self.event_id, now, now, uuid4()),
        )
        for customer_id in (self.customer_id, self.other_customer_id):
            cursor.execute(
                """
                INSERT INTO customers
                    (customer_id, email_hash, persona, home_country,
                     registered_at, first_event_id, last_event_id)
                VALUES (%s, 'hash', 'normal', 'TR', %s, %s, %s)
                """,
                (customer_id, now, self.event_id, self.event_id),
            )
        cursor.execute(
            """
            INSERT INTO sessions
                (session_id, customer_id, device_id, device_type,
                 ip_address, country, channel, started_at, first_event_id)
            VALUES (%s, %s, 'device', 'desktop', '198.51.100.1', 'TR',
                    'web', %s, %s)
            """,
            (self.session_id, self.customer_id, now, self.event_id),
        )
        cursor.execute(
            """
            INSERT INTO carts
                (cart_id, customer_id, session_id, status, currency,
                 created_event_id, latest_event_id)
            VALUES (%s, %s, %s, 'active', 'USD', %s, %s)
            """,
            (
                self.cart_id,
                self.customer_id,
                self.session_id,
                self.event_id,
                self.event_id,
            ),
        )
        for order_id, owner in (
            (self.order_id, self.customer_id),
            (self.order_of_other_customer_id, self.other_customer_id),
        ):
            cursor.execute(
                """
                INSERT INTO orders
                    (order_id, customer_id, session_id, cart_id, status,
                     subtotal, discount, total, currency, item_count,
                     shipping_country, billing_country, created_event_id,
                     latest_event_id, ordered_at)
                VALUES (%s, %s, %s, %s, 'placed', 100, 0, 100, 'USD', 1,
                        'TR', 'DE', %s, %s, %s)
                """,
                (
                    order_id,
                    owner,
                    self.session_id,
                    self.cart_id,
                    self.event_id,
                    self.event_id,
                    now,
                ),
            )


def _legacy_customer_and_order(
    cursor: psycopg.Cursor[tuple[object, ...]],
    customer_id: object,
    order_id: object | None,
) -> tuple[str, tuple[object, ...] | None]:
    """The exact two-query sequence this experiment replaces, kept here
    only as the historical comparison baseline for this test."""
    cursor.execute(
        "SELECT home_country FROM customers WHERE customer_id = %s",
        (customer_id,),
    )
    customer = cursor.fetchone()
    if customer is None:
        raise FraudContextDependencyError("customer fraud context is unavailable")
    home_country = str(customer[0])
    cursor.execute(
        "SELECT ordered_at, total, currency, billing_country "
        "FROM orders WHERE order_id = %s",
        (order_id,),
    )
    order = cursor.fetchone()
    return home_country, order


@pytest.mark.parametrize(
    "order_selector",
    ["own_order", "no_order", "missing_order", "other_customers_order"],
)
def test_combined_query_matches_legacy_two_query_sequence(
    connection: psycopg.Connection[tuple[object, ...]], order_selector: str
) -> None:
    with connection.cursor() as cursor:
        fixture = Fixture(cursor)
        order_id = {
            "own_order": fixture.order_id,
            "no_order": None,
            "missing_order": fixture.missing_order_id,
            # The original code never scoped the order lookup by
            # customer_id; this case proves the consolidated query keeps
            # that exact (non-obvious) behavior rather than "fixing" it.
            "other_customers_order": fixture.order_of_other_customer_id,
        }[order_selector]

        legacy = _legacy_customer_and_order(cursor, fixture.customer_id, order_id)
        combined = FraudContextBuilder._customer_and_order(
            cursor, fixture.customer_id, order_id
        )
        assert combined == legacy


def test_combined_query_raises_for_unknown_customer(
    connection: psycopg.Connection[tuple[object, ...]],
) -> None:
    with connection.cursor() as cursor:
        Fixture(cursor)
        unknown_customer_id = uuid4()
        with pytest.raises(FraudContextDependencyError):
            _legacy_customer_and_order(cursor, unknown_customer_id, None)
        with pytest.raises(FraudContextDependencyError):
            FraudContextBuilder._customer_and_order(cursor, unknown_customer_id, None)


class _CountingCursor:
    """Wraps a real cursor to count execute() calls without changing
    behavior - used only to prove the round-trip reduction, not to fake
    query results."""

    def __init__(self, cursor: psycopg.Cursor[tuple[object, ...]]) -> None:
        self._cursor = cursor
        self.execute_count = 0

    def execute(self, *args: object, **kwargs: object) -> object:
        self.execute_count += 1
        return self._cursor.execute(*args, **kwargs)  # type: ignore[arg-type]

    def __getattr__(self, name: str) -> object:
        return getattr(self._cursor, name)


def test_combined_query_issues_exactly_one_round_trip(
    connection: psycopg.Connection[tuple[object, ...]],
) -> None:
    with connection.cursor() as raw_cursor:
        fixture = Fixture(raw_cursor)
        counting = _CountingCursor(raw_cursor)
        FraudContextBuilder._customer_and_order(
            counting,  # type: ignore[arg-type]
            fixture.customer_id,
            fixture.order_id,
        )
        assert counting.execute_count == 1


def test_legacy_sequence_issues_two_round_trips(
    connection: psycopg.Connection[tuple[object, ...]],
) -> None:
    with connection.cursor() as raw_cursor:
        fixture = Fixture(raw_cursor)
        counting = _CountingCursor(raw_cursor)
        _legacy_customer_and_order(
            counting,  # type: ignore[arg-type]
            fixture.customer_id,
            fixture.order_id,
        )
        assert counting.execute_count == 2
