from scripts.benchmark.direct_saturation import (
    rate_artifact_filename,
    rate_artifact_path,
)


def test_different_rates_produce_different_filenames() -> None:
    assert rate_artifact_filename(1050) != rate_artifact_filename(1075)
    assert rate_artifact_filename(1050) == "direct-saturation-1050.json"
    assert rate_artifact_filename(1075) == "direct-saturation-1075.json"


def test_sequential_invocations_under_one_run_tag_cannot_collide() -> None:
    """The Stage 20 bug: three sequential single-rate invocations under one
    --run-tag all wrote the same 'direct-saturation.json', so the last
    invocation silently destroyed the earlier two rates' results. Every
    rate must now resolve to a distinct path under the same phase_dir."""
    phase_dir = "artifacts/benchmark/bench-example"
    paths = {rate_artifact_path(phase_dir, rate) for rate in (1050, 1075, 1100)}
    assert len(paths) == 3


def test_same_rate_reruns_intentionally_share_a_path() -> None:
    """Rerunning the *same* rate under the same run tag is expected to
    overwrite - that mirrors '--repeats' already being bundled inside one
    rate's own file, not a naming defect."""
    phase_dir = "artifacts/benchmark/bench-example"
    assert rate_artifact_path(phase_dir, 1050) == rate_artifact_path(phase_dir, 1050)


def test_path_is_scoped_under_the_given_phase_dir() -> None:
    path = rate_artifact_path("artifacts/benchmark/bench-example", 900)
    assert path.parent.as_posix() == "artifacts/benchmark/bench-example"
    assert path.name == "direct-saturation-900.json"


def test_custom_prefix_is_respected() -> None:
    assert (
        rate_artifact_filename(950, prefix="postgres-diagnostics-raw")
        == "postgres-diagnostics-raw-950.json"
    )


def test_negative_and_zero_rate_values_still_produce_distinct_paths() -> None:
    # Pathological but not something the CLI validates today; naming must
    # not silently collapse these into the same filename as a positive rate.
    assert rate_artifact_filename(0) == "direct-saturation-0.json"
    assert rate_artifact_filename(-1) != rate_artifact_filename(1)
