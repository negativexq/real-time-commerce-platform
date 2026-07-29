"""Bounded PostgreSQL history context for one persisted source event."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import cast
from uuid import UUID

import psycopg

from services.event_processor.errors import FraudContextDependencyError
from services.event_processor.fraud.config import FraudConfig
from shared.commerce_common.enums import EventType
from shared.schemas import EventEnvelope
from shared.schemas.base import ContractModel


@dataclass(frozen=True, slots=True)
class FraudContext:
    source_event_id: UUID
    event_type: EventType
    event_time: datetime
    customer_id: UUID
    order_id: UUID | None
    payment_id: UUID | None
    session_id: UUID | None
    amount: Decimal | None
    currency: str | None
    payment_status: str | None
    payment_method: str | None
    current_device: str | None
    current_country: str | None
    home_country: str
    session_started_at: datetime | None
    order_time: datetime | None
    historical_average_amount: Decimal | None
    recent_payment_attempts: int
    recent_failed_payments: int
    recent_failure_reasons: int
    recent_distinct_devices: int
    recent_distinct_countries: int
    recent_orders: int
    recent_refunds: int
    lifetime_successful_payments: int
    lifetime_failed_payments: int
    lifetime_spend: Decimal
    refundable_amount: Decimal | None
    previous_successful_devices: frozenset[str]
    previous_successful_countries: frozenset[str]
    product_view_count: int
    refund_amount: Decimal | None
    seconds_since_payment: int | None
    payment_amount_matches_order: bool

    @property
    def established_history(self) -> bool:
        return self.lifetime_successful_payments > 0

    @property
    def is_new_device(self) -> bool:
        return (
            self.current_device is not None
            and bool(self.previous_successful_devices)
            and self.current_device not in self.previous_successful_devices
        )

    @property
    def country_mismatch(self) -> bool:
        return (
            self.current_country is not None
            and self.current_country != self.home_country
        )

    @property
    def checkout_seconds(self) -> int | None:
        if self.session_started_at is None:
            return None
        return max(0, int((self.event_time - self.session_started_at).total_seconds()))


class FraudContextBuilder:
    """Build one snapshot using event-time cutoffs and bounded row limits."""

    def __init__(self, config: FraudConfig) -> None:
        self.config = config

    def build(
        self,
        connection: psycopg.Connection[tuple[object, ...]],
        event: EventEnvelope[ContractModel],
    ) -> FraudContext:
        payload = event.payload
        customer_id = getattr(payload, "customer_id", None)
        if not isinstance(customer_id, UUID):
            raise FraudContextDependencyError("eligible event has no customer identity")
        order_id = self._uuid(payload, "order_id")
        payment_id = self._uuid(payload, "payment_id")
        session_id = self._uuid(payload, "session_id")
        amount = self._decimal(payload, "amount") or self._decimal(
            payload, "total_amount"
        )
        currency_value = getattr(payload, "currency", None)
        currency = getattr(currency_value, "value", None)
        device = getattr(payload, "device_id", None)
        country = getattr(payload, "country_code", None)
        method = getattr(getattr(payload, "payment_method", None), "value", None)
        lookback = event.event_time - timedelta(
            days=self.config.fraud_history_lookback_days
        )
        limit = self.config.fraud_max_history_records
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT home_country FROM customers WHERE customer_id = %s",
                (customer_id,),
            )
            customer = cursor.fetchone()
            if customer is None:
                raise FraudContextDependencyError(
                    "customer fraud context is unavailable"
                )
            home_country = str(customer[0])
            if session_id is None and order_id is not None:
                cursor.execute(
                    "SELECT session_id FROM orders WHERE order_id = %s", (order_id,)
                )
                row = cursor.fetchone()
                session_id = cast(UUID | None, row[0] if row else None)
            cursor.execute(
                "SELECT started_at FROM sessions WHERE session_id = %s",
                (session_id,),
            )
            row = cursor.fetchone()
            session_started = cast(datetime | None, row[0] if row else None)
            cursor.execute(
                """
                SELECT ordered_at, total, currency, billing_country
                FROM orders WHERE order_id = %s
                """,
                (order_id,),
            )
            order = cursor.fetchone()
            order_time = cast(datetime | None, order[0] if order else None)
            order_matches = (
                order is None
                or amount is None
                or (cast(Decimal, order[1]) == amount and str(order[2]) == currency)
            )
            cursor.execute(
                """
                SELECT status, amount, currency, device_id, country, attempted_at,
                       failure_reason
                FROM payments
                WHERE customer_id = %s AND attempted_at <= %s
                  AND attempted_at >= %s
                ORDER BY attempted_at DESC, payment_id
                LIMIT %s
                """,
                (customer_id, event.event_time, lookback, limit),
            )
            payments = cursor.fetchall()
            cursor.execute(
                """
                SELECT status, amount, currency, device_id, country, attempted_at
                FROM payments
                WHERE customer_id = %s AND attempted_at < %s
                ORDER BY attempted_at DESC, payment_id
                LIMIT %s
                """,
                (customer_id, event.event_time, limit),
            )
            prior_payments = cursor.fetchall()
            cursor.execute(
                """
                SELECT requested_at, amount FROM refunds
                WHERE customer_id = %s AND requested_at <= %s AND requested_at >= %s
                ORDER BY requested_at DESC, refund_id LIMIT %s
                """,
                (customer_id, event.event_time, lookback, limit),
            )
            refunds = cursor.fetchall()
            cursor.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT 1 FROM orders
                    WHERE customer_id = %s AND ordered_at <= %s AND ordered_at >= %s
                    ORDER BY ordered_at DESC LIMIT %s
                ) bounded
                """,
                (customer_id, event.event_time, lookback, limit),
            )
            recent_orders = cast(int, cast(tuple[object, ...], cursor.fetchone())[0])
            cursor.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT 1 FROM product_views
                    WHERE customer_id = %s AND viewed_at <= %s AND viewed_at >= %s
                    ORDER BY viewed_at DESC LIMIT %s
                ) bounded
                """,
                (
                    customer_id,
                    event.event_time,
                    session_started or lookback,
                    limit,
                ),
            )
            view_count = cast(int, cast(tuple[object, ...], cursor.fetchone())[0])
            refundable, seconds_since_payment = self._refund_facts(
                cursor, payment_id, event.event_time
            )
        successful = [row for row in prior_payments if str(row[0]) == "completed"]
        matching_amounts = [
            cast(Decimal, row[1])
            for row in successful
            if currency is not None and str(row[2]) == currency
        ]
        average = (
            sum(matching_amounts, Decimal("0")) / len(matching_amounts)
            if matching_amounts
            else None
        )
        velocity_start = event.event_time - timedelta(
            seconds=self.config.fraud_velocity_window_seconds
        )
        failure_start = event.event_time - timedelta(
            seconds=self.config.fraud_failed_payment_window_seconds
        )
        country_start = event.event_time - timedelta(
            seconds=self.config.fraud_country_window_seconds
        )
        velocity = [row for row in payments if cast(datetime, row[5]) >= velocity_start]
        failures = [
            row
            for row in payments
            if str(row[0]) == "failed" and cast(datetime, row[5]) >= failure_start
        ]
        countries = {
            str(row[4]) for row in payments if cast(datetime, row[5]) >= country_start
        }
        return FraudContext(
            event.event_id,
            event.event_type,
            event.event_time,
            customer_id,
            order_id,
            payment_id,
            session_id,
            amount,
            currency,
            "completed"
            if event.event_type is EventType.PAYMENT_COMPLETED
            else ("failed" if event.event_type is EventType.PAYMENT_FAILED else None),
            method,
            str(device) if device is not None else None,
            str(country)
            if country is not None
            else (
                str(order[3])
                if order is not None and event.event_type is EventType.ORDER_CREATED
                else None
            ),
            home_country,
            session_started,
            order_time,
            average,
            len(velocity),
            len(failures),
            len({str(row[6]) for row in failures if row[6] is not None}),
            len({str(row[3]) for row in velocity}),
            len(countries),
            recent_orders,
            len(refunds),
            len(successful),
            sum(str(row[0]) == "failed" for row in prior_payments),
            sum((cast(Decimal, row[1]) for row in successful), Decimal("0")),
            refundable,
            frozenset(str(row[3]) for row in successful),
            frozenset(str(row[4]) for row in successful),
            view_count,
            self._decimal(payload, "amount")
            if event.event_type is EventType.REFUND_REQUESTED
            else None,
            seconds_since_payment,
            order_matches,
        )

    @staticmethod
    def _uuid(payload: object, name: str) -> UUID | None:
        value = getattr(payload, name, None)
        return value if isinstance(value, UUID) else None

    @staticmethod
    def _decimal(payload: object, name: str) -> Decimal | None:
        value = getattr(payload, name, None)
        return value if isinstance(value, Decimal) else None

    @staticmethod
    def _refund_facts(
        cursor: psycopg.Cursor[tuple[object, ...]],
        payment_id: UUID | None,
        event_time: datetime,
    ) -> tuple[Decimal | None, int | None]:
        if payment_id is None:
            return None, None
        cursor.execute(
            """
            SELECT p.amount - COALESCE(SUM(r.amount), 0), p.attempted_at
            FROM payments p LEFT JOIN refunds r ON r.payment_id = p.payment_id
            WHERE p.payment_id = %s GROUP BY p.amount, p.attempted_at
            """,
            (payment_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None, None
        return cast(Decimal, row[0]), max(
            0, int((event_time - cast(datetime, row[1])).total_seconds())
        )
