from itertools import pairwise

import pytest

from movies_pipeline.exceptions import PartitionPlanError
from movies_pipeline.sources.kinopoisk import (
    count_kinopoisk,
    parse_range,
    plan_partitions,
    split_range,
)


def counting_api(votes, calls=None):
    def fetch(endpoint, *, api_key, params, session):
        if calls is not None:
            calls.append(dict(params))
        low, high = parse_range(params.get("votes.kp", "0-1000000000"))
        return {"docs": [], "total": sum(low <= v <= high for v in votes)}

    return fetch


def plan(votes, partitions, *, max_records=100, max_count_requests=20, calls=None):
    return plan_partitions(
        "/movie",
        {"type": "movie", "limit": 250},
        partitions,
        split_key="votes.kp",
        max_records=max_records,
        max_count_requests=max_count_requests,
        session=object(),
        fetch=counting_api(votes, calls),
    )


def test_partitions_that_fit_are_kept_as_is():
    votes = [1000] * 50 + [3000] * 80
    planned, used = plan(votes, [{"votes.kp": "1000-1999"}, {"votes.kp": "2000-4999"}])

    assert planned == [
        {"partition": {"votes.kp": "1000-1999"}, "total": 50},
        {"partition": {"votes.kp": "2000-4999"}, "total": 80},
    ]
    assert used == 2


def test_overflowing_partition_is_split_until_it_fits():
    votes = list(range(1000, 2000))
    planned, used = plan(
        votes,
        [{"votes.kp": "1000-1999"}, {"votes.kp": "2000-4999"}],
        max_count_requests=50,
    )

    ranges = [item["partition"]["votes.kp"] for item in planned]
    assert ranges[-1] == "2000-4999"
    bounds = [parse_range(r) for r in ranges[:-1]]
    assert bounds[0][0] == 1000
    assert bounds[-1][1] == 1999
    assert all(prev[1] + 1 == nxt[0] for prev, nxt in pairwise(bounds))
    assert all(item["total"] <= 100 for item in planned)
    assert sum(item["total"] for item in planned) == 1000
    seeds = 2
    splits = len(planned) - seeds
    assert used == seeds + splits


def test_count_request_is_cheap_and_keeps_filters():
    calls = []
    plan([1000], [{"votes.kp": "1000-1999"}], calls=calls)

    assert calls == [
        {
            "type": "movie",
            "limit": 1,
            "selectFields": ["id"],
            "withCount": "true",
            "votes.kp": "1000-1999",
        }
    ]


def test_single_value_range_that_overflows_fails():
    with pytest.raises(PartitionPlanError, match="cannot be split"):
        plan([1500] * 101, [{"votes.kp": "1500-1500"}])


def test_overflow_without_split_key_fails():
    with pytest.raises(PartitionPlanError, match="no 'votes.kp' range"):
        plan([1500] * 101, [{}])


def test_count_request_budget_is_enforced():
    with pytest.raises(PartitionPlanError, match="max_count_requests=3"):
        plan(list(range(1000, 2000)), [{"votes.kp": "1000-1999"}], max_count_requests=3)


def test_count_requires_integer_total():
    def fetch(endpoint, *, api_key, params, session):
        return {"docs": []}

    with pytest.raises(TypeError, match="total"):
        count_kinopoisk("/movie", session=object(), fetch=fetch)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("1000-1999", ("1000-1413", "1414-1999")),
        ("20000-100000000", ("20000-1414213", "1414214-100000000")),
        ("0-10", ("0-3", "4-10")),
        ("1-2", ("1-1", "2-2")),
    ],
)
def test_split_range_halves_without_gaps(value, expected):
    assert split_range(value) == expected


@pytest.mark.parametrize("value", ["1000", "a-b", "2000-1000", "1-2-3"])
def test_parse_range_rejects_bad_values(value):
    with pytest.raises(ValueError):
        parse_range(value)


def test_heavy_tailed_range_converges_quickly():
    votes = [int(20000 * 1.0005**i) for i in range(6000)]
    calls = []
    planned, used = plan(
        votes,
        [{"votes.kp": "20000-100000000"}],
        max_records=2250,
        max_count_requests=10,
        calls=calls,
    )

    assert sum(item["total"] for item in planned) == 6000
    assert all(item["total"] <= 2250 for item in planned)
    assert used == len(calls) <= 10


def test_derived_totals_match_real_counts():
    votes = [1000 + (i * 7) % 1000 for i in range(900)]
    planned, _ = plan(
        votes, [{"votes.kp": "1000-1999"}], max_records=100, max_count_requests=50
    )

    for item in planned:
        low, high = parse_range(item["partition"]["votes.kp"])
        assert item["total"] == sum(low <= v <= high for v in votes)
