from datetime import UTC, datetime


def _to_utc_midnight(value: datetime, name: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    value_utc = value.astimezone(UTC)
    if (value_utc.hour, value_utc.minute, value_utc.second, value_utc.microsecond) != (
        0,
        0,
        0,
        0,
    ):
        raise ValueError(
            f"{name}={value.isoformat()} is not midnight UTC; updatedAt filter has day precision"
        )
    return value_utc


def updated_at_filter(start: datetime, end: datetime) -> str:
    start_utc = _to_utc_midnight(start, "start")
    end_utc = _to_utc_midnight(end, "end")
    if end_utc <= start_utc:
        raise ValueError(
            f"empty interval: start={start.isoformat()}, end={end.isoformat()}"
        )
    return f"{start_utc:%d.%m.%Y}-{end_utc:%d.%m.%Y}"
