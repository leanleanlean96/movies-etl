from datetime import UTC, datetime, timedelta, timezone

import pytest

from movies_pipeline.interval import updated_at_filter

MSK = timezone(timedelta(hours=3))


def utc(*args):
    return datetime(*args, tzinfo=UTC)


def test_one_day_window():
    assert (
        updated_at_filter(utc(2026, 10, 22), utc(2026, 10, 23))
        == "22.10.2026-23.10.2026"
    )


def test_month_boundary():
    assert (
        updated_at_filter(utc(2026, 10, 31), utc(2026, 11, 1))
        == "31.10.2026-01.11.2026"
    )


def test_year_boundary():
    assert (
        updated_at_filter(utc(2026, 12, 31), utc(2027, 1, 1)) == "31.12.2026-01.01.2027"
    )


def test_multi_day_window():
    assert (
        updated_at_filter(utc(1990, 1, 1), utc(2026, 10, 20)) == "01.01.1990-20.10.2026"
    )


def test_other_timezone_at_utc_midnight_is_converted():
    start = datetime(2026, 10, 22, 3, 0, tzinfo=MSK)
    end = datetime(2026, 10, 23, 3, 0, tzinfo=MSK)

    assert updated_at_filter(start, end) == "22.10.2026-23.10.2026"


def test_moscow_midnight_is_not_utc_midnight():
    with pytest.raises(ValueError, match="not midnight UTC"):
        updated_at_filter(
            datetime(2026, 10, 22, tzinfo=MSK), datetime(2026, 10, 23, tzinfo=MSK)
        )


@pytest.mark.parametrize(
    "start, end",
    [
        (utc(2026, 10, 22, 12), utc(2026, 10, 23)),
        (utc(2026, 10, 22), utc(2026, 10, 23, 0, 0, 1)),
        (utc(2026, 10, 22), utc(2026, 10, 23, 0, 0, 0, 1)),
    ],
)
def test_bounds_must_be_utc_midnight(start, end):
    with pytest.raises(ValueError, match="not midnight UTC"):
        updated_at_filter(start, end)


def test_naive_datetime_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        updated_at_filter(datetime(2026, 10, 22), utc(2026, 10, 23))


@pytest.mark.parametrize(
    "start, end",
    [
        (utc(2026, 10, 22), utc(2026, 10, 22)),
        (utc(2026, 10, 23), utc(2026, 10, 22)),
    ],
)
def test_empty_or_reversed_interval_is_rejected(start, end):
    with pytest.raises(ValueError, match="empty interval"):
        updated_at_filter(start, end)
