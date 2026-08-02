"""Small percentile helper (linear interpolation, no numpy dependency)."""


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = q * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    fraction = rank - low
    return ordered[low] + (ordered[high] - ordered[low]) * fraction


def percentiles(
    values: list[float], qs: tuple[float, ...] = (0.5, 0.95, 0.99)
) -> dict[str, float | None]:
    return {f"p{int(q * 100)}": percentile(values, q) for q in qs}
