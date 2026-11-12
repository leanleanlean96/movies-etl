from collections.abc import Mapping

from pyspark.sql.dataframe import DataFrame

from .constants import QUARANTINE_REASONS, TOTAL_PARTS
from .exceptions import ReconciliationError


def reconcile_totals(stats: Mapping[str, int]) -> None:
    missing = [name for name in ("read", *TOTAL_PARTS) if name not in stats]
    if missing:
        raise ReconciliationError(f"stats are missing {missing}")
    accounted = sum(stats[name] for name in TOTAL_PARTS)
    if stats["read"] != accounted:
        parts = " + ".join(f"{name}={stats[name]}" for name in TOTAL_PARTS)
        raise ReconciliationError(f"read={stats['read']} but {parts} = {accounted}")


def collect_stats(
    checked: DataFrame,
    tables: Mapping[str, DataFrame],
    duplicates: int,
    expected_records: int,
) -> dict[str, int]:
    reasons = {
        row["reason"]: row["count"]
        for row in checked.groupBy("reason").count().collect()
    }
    unknown = set(reasons) - {None, *QUARANTINE_REASONS}
    if unknown:
        raise ReconciliationError(f"unknown quarantine reasons: {sorted(unknown)}")
    stats = {
        "read": sum(reasons.values()),
        "quarantined": tables["quarantine"].count(),
        "duplicates": duplicates,
        "movies": tables["movies"].count(),
        "genre_links": tables["movie_genres"].count(),
        "country_links": tables["movie_countries"].count(),
        **{
            f"quarantined_{reason}": reasons.get(reason, 0)
            for reason in QUARANTINE_REASONS
        },
    }
    if stats["read"] != expected_records:
        raise ReconciliationError(
            f"read={stats['read']} but manifest says {expected_records}"
        )
    expected_quarantined = sum(reasons.get(reason, 0) for reason in QUARANTINE_REASONS)
    if stats["quarantined"] != expected_quarantined:
        raise ReconciliationError(
            f"quarantined={stats['quarantined']} but reasons sum to {expected_quarantined}"
        )
    reconcile_totals(stats)
    return stats
