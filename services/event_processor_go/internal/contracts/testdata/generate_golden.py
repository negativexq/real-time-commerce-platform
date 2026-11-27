"""Regenerate golden.json from the Python reference implementation.

Run from the repository root:
    .venv/bin/python \
        services/event_processor_go/internal/contracts/testdata/generate_golden.py

Each case records whether shared.schemas.parse_event accepts the input and,
if so, the exact canonical_json() text and its SHA-256. The Go contracts
package must reproduce both byte for byte so Python and Go processors agree
on processed_events.payload_sha256 for the same event.
"""

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

from shared.schemas import canonical_json, parse_event

ROOT = Path(__file__).resolve().parents[5]
FIXTURES = ROOT / "tests" / "fixtures" / "events"
OUT = Path(__file__).with_name("golden.json")

C = "00000000-0000-4000-8000-000000000101"
S = "00000000-0000-4000-8000-000000000201"
CART = "00000000-0000-4000-8000-000000000301"
ORDER = "00000000-0000-4000-8000-000000000401"
PAY = "00000000-0000-4000-8000-000000000501"


def envelope(event_type: str, payload: Any, **overrides: object) -> dict[str, Any]:
    base: dict[str, Any] = {
        "event_id": "00000000-0000-4000-8000-000000000001",
        "event_type": event_type,
        "event_version": 1,
        "event_time": "2026-01-15T10:00:00Z",
        "produced_at": "2026-01-15T10:00:01Z",
        "source": "golden",
        "correlation_id": "00000000-0000-4000-8000-000000000100",
        "payload": payload,
    }
    base.update(overrides)
    return base


def session(**over: object) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "session_id": S,
        "customer_id": C,
        "device_id": "00000000-0000-4000-8000-000000000601",
        "device_type": "mobile",
        "ip_address": "203.0.113.7",
        "country_code": "TR",
        "channel": "web",
        "started_at": "2026-01-15T09:59:00Z",
    }
    payload.update(over)
    return envelope("session_started", payload)


def checkout(**over: object) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "session_id": S,
        "customer_id": C,
        "cart_id": CART,
        "item_count": 2,
        "subtotal": "100.00",
        "discount_amount": "10.00",
        "total_amount": "90.00",
        "currency": "EUR",
    }
    payload.update(over)
    return envelope("checkout_started", payload)


def product(**over: object) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "session_id": S,
        "customer_id": C,
        "product_id": "00000000-0000-4000-8000-000000000701",
        "category": "electronics",
        "unit_price": "19.99",
        "currency": "USD",
        "quantity_available": 5,
    }
    payload.update(over)
    return envelope("product_viewed", payload)


def cases() -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    for path in sorted(FIXTURES.glob("*.json")):
        out.append((f"fixture/{path.name}", path.read_bytes()))

    def add(name: str, value: object) -> None:
        if isinstance(value, bytes):
            out.append((name, value))
        else:
            out.append((name, json.dumps(value, ensure_ascii=False).encode()))

    add("session/basic", session())
    add("session/ipv6", session(ip_address="2001:DB8:0:0:0:0:0:1"))
    add("session/ipv4_mapped", session(ip_address="::ffff:1.2.3.4"))
    add("session/ipv4_compat", session(ip_address="::1.2.3.4"))
    add(
        "session/ipv6_zeros",
        session(ip_address="2001:0db8:0000:0000:0001:0000:0000:0001"),
    )
    add("session/bad_ip_leading_zero", session(ip_address="01.2.3.4"))
    add("session/offset_time", session(started_at="2026-01-15T12:59:00+03:00"))
    add("session/fraction_time", session(started_at="2026-01-15T09:59:00.123Z"))
    add("session/fraction_long", session(started_at="2026-01-15T09:59:00.1234567Z"))
    add("session/naive_time", session(started_at="2026-01-15T09:59:00"))
    add("session/lower_t", session(started_at="2026-01-15t09:59:00z"))
    add("session/no_seconds", session(started_at="2026-01-15T09:59Z"))
    add("session/compact_offset", session(started_at="2026-01-15T09:59:00-0130"))
    add("session/upper_uuid", session(device_id="ABCDEF00-0000-4000-8000-000000000601"))
    add("session/simple_uuid", session(device_id="abcdef00000040008000000000000601"))
    add("session/bad_country", session(country_code="tr"))
    add("session/bad_enum", session(channel="email"))
    add("session/extra_field", session(extra=True))

    add("checkout/basic", checkout())
    add(
        "checkout/numbers",
        checkout(subtotal=100, discount_amount=10.5, total_amount=89.5),
    )
    add(
        "checkout/scientific",
        checkout(subtotal="1E+2", discount_amount="0", total_amount="100"),
    )
    add(
        "checkout/tiny",
        checkout(subtotal="0.0000001", discount_amount="0", total_amount="1E-7"),
    )
    add(
        "checkout/spaces",
        checkout(subtotal=" 100.00 ", discount_amount="10", total_amount="90.000"),
    )
    add(
        "checkout/underscore",
        checkout(subtotal="1_000.00", discount_amount="0", total_amount="1000"),
    )
    add("checkout/bad_total", checkout(total_amount="91.00"))
    add("checkout/discount_gt", checkout(discount_amount="200", total_amount="-100"))
    add(
        "checkout/negative",
        checkout(subtotal="-1", discount_amount="0", total_amount="-1"),
    )
    add("checkout/nan", checkout(subtotal="NaN"))
    add("checkout/int_as_float", checkout(item_count=2.0))
    add("checkout/int_as_string", checkout(item_count="2"))
    add("checkout/bool_count", checkout(item_count=True))
    add("checkout/zero_count", checkout(item_count=0))
    add(
        "checkout/big_exponent",
        checkout(
            subtotal="12345678901234567890.123456789",
            discount_amount="0.123456789",
            total_amount="12345678901234567890.000000000",
        ),
    )

    add("product/unicode", product(category='  ev & bahçe — "özel" \\ \n\t\u0001  '))
    add("product/emoji", product(category="kitap 📚"))
    add("product/blank_category", product(category="   "))
    add("product/zero_price", product(unit_price="0"))
    add("product/neg_zero_price", product(unit_price="-0.00"))
    add("product/zero_stock", product(quantity_available=0))

    add(
        "envelope/source_strip",
        envelope("product_viewed", product()["payload"], source="  golden  "),
    )
    add(
        "envelope/clock_skew",
        envelope(
            "product_viewed", product()["payload"], produced_at="2026-01-15T09:54:59Z"
        ),
    )
    add(
        "envelope/clock_skew_edge",
        envelope(
            "product_viewed", product()["payload"], produced_at="2026-01-15T09:55:00Z"
        ),
    )
    add(
        "envelope/version_zero",
        envelope("product_viewed", product()["payload"], event_version=0),
    )
    add(
        "envelope/missing_source", {k: v for k, v in product().items() if k != "source"}
    )
    add("envelope/payload_list", envelope("product_viewed", []))
    add("envelope/not_object", b"[1, 2]")
    add(
        "envelope/duplicate_key", b'{"source":"a",' + json.dumps(product()).encode()[1:]
    )

    alert = json.loads((FIXTURES / "fraud_alert_created.json").read_text())
    nulls = copy.deepcopy(alert)
    nulls["payload"]["customer_id"] = None
    nulls["payload"]["order_id"] = None
    add("alert/null_ids", nulls)
    empty = copy.deepcopy(alert)
    empty["payload"]["reasons"] = []
    add("alert/empty_reasons", empty)
    high = copy.deepcopy(alert)
    high["payload"]["fraud_score"] = "100.5"
    add("alert/score_high", high)
    return out


def main() -> None:
    results = []
    for name, raw in cases():
        try:
            event = parse_event(raw)
        except Exception as exc:  # noqa: BLE001 - recording rejection only
            results.append(
                {
                    "name": name,
                    "input": raw.decode(),
                    "ok": False,
                    "error": type(exc).__name__,
                }
            )
            continue
        canonical = canonical_json(event)
        payload_customer = getattr(event.payload, "customer_id", None)
        results.append(
            {
                "name": name,
                "input": raw.decode(),
                "ok": True,
                "canonical": canonical,
                "sha256": hashlib.sha256(canonical.encode()).hexdigest(),
                "key": str(
                    payload_customer
                    if payload_customer is not None
                    else event.correlation_id
                ),
            }
        )
    OUT.write_text(json.dumps(results, ensure_ascii=False, indent=1) + "\n")
    print(f"wrote {len(results)} cases to {OUT}")


if __name__ == "__main__":
    main()
