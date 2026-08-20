from services.event_processor.consumer import broker_rtt_avg_ms, fetchq_records_total


def _stats(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "topics": {
            "commerce.events": {
                "partitions": {
                    "0": {"fetchq_cnt": 12},
                    "1": {"fetchq_cnt": 3},
                    "2": {"fetchq_cnt": 0},
                }
            }
        },
        "brokers": {
            "localhost:29092/1": {"rtt": {"avg": 2000, "cnt": 5}},
            "GroupCoordinator": {"rtt": {"avg": 0, "cnt": 0}},
        },
    }
    base.update(overrides)
    return base


def test_fetchq_records_total_sums_all_partitions() -> None:
    assert fetchq_records_total(_stats()) == 15


def test_fetchq_records_total_empty_topics_is_zero() -> None:
    assert fetchq_records_total({}) == 0


def test_fetchq_records_total_ignores_missing_or_non_int_fields() -> None:
    stats = _stats(
        topics={
            "commerce.events": {
                "partitions": {
                    "0": {"fetchq_cnt": 5},
                    "1": {},
                    "2": {"fetchq_cnt": None},
                }
            }
        }
    )
    assert fetchq_records_total(stats) == 5


def test_broker_rtt_avg_ms_converts_microseconds_and_ignores_zero_count() -> None:
    # Only the broker with cnt > 0 counts; 2000us -> 2.0ms.
    assert broker_rtt_avg_ms(_stats()) == 2.0


def test_broker_rtt_avg_ms_averages_multiple_reporting_brokers() -> None:
    stats = _stats(
        brokers={
            "b1": {"rtt": {"avg": 1000, "cnt": 3}},
            "b2": {"rtt": {"avg": 3000, "cnt": 2}},
        }
    )
    assert broker_rtt_avg_ms(stats) == 2.0


def test_broker_rtt_avg_ms_none_when_no_broker_has_reported() -> None:
    stats = _stats(brokers={"b1": {"rtt": {"avg": 0, "cnt": 0}}, "b2": {}})
    assert broker_rtt_avg_ms(stats) is None


def test_broker_rtt_avg_ms_none_for_empty_stats() -> None:
    assert broker_rtt_avg_ms({}) is None
