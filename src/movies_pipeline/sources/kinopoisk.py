import logging
import math
import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from movies_pipeline.exceptions import (
    MaxPagesBudgetExceeded,
    PartitionPlanError,
    QueryPageLimitExceeded,
)

logger = logging.getLogger(__name__)

KINOPOISK_API_BASE_URL = "https://api.poiskkino.dev/v1.5"
KINOPOISK_API_KEY_ENV = "KINOPOISK_API_KEY"
FREE_PLAN_MAX_PAGES_PER_QUERY = 10
ERROR_BODY_LIMIT = 500
READ_TIMEOUT_SECONDS = 60

MOVIE_FIELDS = (
    "id",
    "externalId",
    "name",
    "enName",
    "alternativeName",
    "names",
    "description",
    "shortDescription",
    "slogan",
    "type",
    "typeNumber",
    "isSeries",
    "status",
    "year",
    "releaseYears",
    "rating",
    "ratingMpaa",
    "ageRating",
    "votes",
    "seasonsInfo",
    "budget",
    "audience",
    "movieLength",
    "seriesLength",
    "totalSeriesLength",
    "genres",
    "countries",
    "poster",
    "backdrop",
    "logo",
    "ticketsOnSale",
    "videos",
    "networks",
    "persons",
    "facts",
    "fees",
    "premiere",
    "similarMovies",
    "sequelsAndPrequels",
    "watchability",
    "lists",
    "top10",
    "top250",
    "updatedAt",
    "createdAt",
)

ParamValue = str | int | float | bool | Sequence[str | int]


def build_kinopoisk_session() -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_kinopoisk(
    endpoint: str,
    *,
    api_key: str | None = None,
    params: Mapping[str, ParamValue] | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    if not endpoint.startswith("/") or endpoint.startswith("//") or "?" in endpoint:
        raise ValueError(
            "endpoint must be a path like '/movie/535341'; pass query values in params"
        )
    if api_key is None:
        api_key = os.environ.get(KINOPOISK_API_KEY_ENV, "")
    if not api_key.strip():
        raise ValueError(
            f"PoiskKino API key is required: pass api_key= or set {KINOPOISK_API_KEY_ENV}"
        )

    client = session if session is not None else build_kinopoisk_session()
    logger.debug("Requesting PoiskKino endpoint %s", endpoint)
    response = client.get(
        f"{KINOPOISK_API_BASE_URL}{endpoint}",
        headers={"X-API-KEY": api_key, "Accept": "application/json"},
        params=params,
        timeout=(5, READ_TIMEOUT_SECONDS),
    )
    if response.status_code >= 400:
        raise requests.HTTPError(
            f"PoiskKino {endpoint} returned {response.status_code} {response.reason}: "
            f"{response.text[:ERROR_BODY_LIMIT]}",
            response=response,
        )
    result = response.json()
    if not isinstance(result, dict):
        raise TypeError(f"Expected a JSON object from PoiskKino endpoint {endpoint}")
    return result


def iter_pages(
    endpoint: str,
    params: Mapping[str, ParamValue] | None = None,
    *,
    max_pages: int,
    api_key: str | None = None,
    session: requests.Session | None = None,
    fetch: Callable[..., dict[str, Any]] = fetch_kinopoisk,
) -> Iterator[dict[str, Any]]:
    if max_pages < 1:
        raise ValueError("max_pages must be >= 1")
    base_params = dict(params or {})
    if "next" in base_params or "prev" in base_params:
        raise ValueError("cursor params (next/prev) are managed by iter_pages")

    own_session = session is None
    client = build_kinopoisk_session() if own_session else session
    try:
        cursor: str | None = None
        for page_no in range(1, max_pages + 1):
            page_params = (
                base_params if cursor is None else {**base_params, "next": cursor}
            )
            page = fetch(endpoint, api_key=api_key, params=page_params, session=client)
            logger.info(
                "PoiskKino %s page %d: %d docs, hasNext=%s",
                endpoint,
                page_no,
                len(page.get("docs") or []),
                page.get("hasNext"),
            )
            yield page

            if not page.get("hasNext"):
                return
            cursor = page.get("next")
            if not cursor:
                raise RuntimeError(
                    f"PoiskKino {endpoint}: hasNext=true but no next cursor on page {page_no}"
                )

        raise MaxPagesBudgetExceeded(
            f"PoiskKino {endpoint}: more pages available after max_pages={max_pages}; "
            "raise max_pages or narrow the interval"
        )
    finally:
        if own_session:
            client.close()


def iter_partitioned_pages(
    endpoint: str,
    params: Mapping[str, ParamValue] | None = None,
    partitions: Sequence[Mapping[str, ParamValue]] | None = None,
    *,
    max_pages_total: int,
    max_pages_per_query: int = FREE_PLAN_MAX_PAGES_PER_QUERY,
    api_key: str | None = None,
    session: requests.Session | None = None,
    fetch: Callable[..., dict[str, Any]] = fetch_kinopoisk,
) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
    if max_pages_total < 1 or max_pages_per_query < 1:
        raise ValueError("max_pages_total and max_pages_per_query must be >= 1")
    base_params = dict(params or {})
    parts = [dict(part) for part in (partitions or [{}])]

    own_session = session is None
    client = build_kinopoisk_session() if own_session else session
    used = 0
    try:
        for query_no, part in enumerate(parts, start=1):
            query = {**base_params, **part}
            remaining = max_pages_total - used
            if remaining < 1:
                raise MaxPagesBudgetExceeded(
                    f"PoiskKino {endpoint}: max_pages_total={max_pages_total} spent before "
                    f"query {query_no}/{len(parts)}; raise max_pages"
                )
            limit = min(max_pages_per_query, remaining)
            logger.info(
                "PoiskKino %s query %d/%d: %s", endpoint, query_no, len(parts), query
            )
            try:
                for page in iter_pages(
                    endpoint,
                    query,
                    max_pages=limit,
                    api_key=api_key,
                    session=client,
                    fetch=fetch,
                ):
                    used += 1
                    yield part, page
            except MaxPagesBudgetExceeded as e:
                if limit < max_pages_per_query:
                    raise MaxPagesBudgetExceeded(
                        f"PoiskKino {endpoint}: max_pages_total={max_pages_total} exceeded "
                        f"in query {query_no}/{len(parts)}; raise max_pages"
                    ) from e
                raise QueryPageLimitExceeded(
                    f"PoiskKino {endpoint}: query {query} has more than {max_pages_per_query} "
                    "pages, which the API plan does not allow; split this partition further"
                ) from e
    finally:
        if own_session:
            client.close()


def count_kinopoisk(
    endpoint: str,
    params: Mapping[str, ParamValue] | None = None,
    *,
    api_key: str | None = None,
    session: requests.Session | None = None,
    fetch: Callable[..., dict[str, Any]] = fetch_kinopoisk,
) -> int:
    count_params = {
        **dict(params or {}),
        "limit": 1,
        "selectFields": ["id"],
        "withCount": "true",
    }
    result = fetch(endpoint, api_key=api_key, params=count_params, session=session)
    total = result.get("total")
    if not isinstance(total, int) or isinstance(total, bool) or total < 0:
        raise TypeError(
            f"PoiskKino {endpoint}: expected integer 'total' in count response, got {total!r}"
        )
    return total


def parse_range(value: ParamValue) -> tuple[int, int]:
    parts = str(value).split("-")
    if len(parts) != 2:
        raise ValueError(f"range {value!r} must look like 'low-high'")
    try:
        low, high = int(parts[0]), int(parts[1])
    except ValueError as e:
        raise ValueError(f"range {value!r} must contain integers") from e
    if low > high:
        raise ValueError(f"range {value!r} has low > high")
    return low, high


def split_range(value: ParamValue) -> tuple[str, str]:
    low, high = parse_range(value)
    if low == high:
        raise ValueError(f"range {value!r} is a single value and cannot be split")
    mid = min(max(math.isqrt(max(low, 1) * high), low), high - 1)
    return f"{low}-{mid}", f"{mid + 1}-{high}"


def plan_partitions(
    endpoint: str,
    params: Mapping[str, ParamValue] | None = None,
    partitions: Sequence[Mapping[str, ParamValue]] | None = None,
    *,
    split_key: str,
    max_records: int,
    max_count_requests: int,
    api_key: str | None = None,
    session: requests.Session | None = None,
    fetch: Callable[..., dict[str, Any]] = fetch_kinopoisk,
) -> tuple[list[dict[str, Any]], int]:
    if max_records < 1 or max_count_requests < 1:
        raise ValueError("max_records and max_count_requests must be >= 1")
    base_params = dict(params or {})
    pending: list[tuple[dict[str, Any], int | None]] = [
        (dict(part), None) for part in (partitions or [{}])
    ]
    planned: list[dict[str, Any]] = []
    used = 0

    own_session = session is None
    client = build_kinopoisk_session() if own_session else session

    def count(part: Mapping[str, Any]) -> int:
        nonlocal used
        if used >= max_count_requests:
            raise PartitionPlanError(
                f"PoiskKino {endpoint}: max_count_requests={max_count_requests} spent "
                f"while splitting by {split_key}; widen the seed partitions or raise the limit"
            )
        total = count_kinopoisk(
            endpoint,
            {**base_params, **part},
            api_key=api_key,
            session=client,
            fetch=fetch,
        )
        used += 1
        logger.info("PoiskKino %s count %s: %d records", endpoint, part, total)
        return total

    try:
        while pending:
            part, total = pending.pop(0)
            if total is None:
                total = count(part)
            if total <= max_records:
                planned.append({"partition": part, "total": total})
                continue
            if split_key not in part:
                raise PartitionPlanError(
                    f"PoiskKino {endpoint}: {part} has {total} records > {max_records} "
                    f"and no {split_key!r} range to split"
                )
            try:
                left, right = split_range(part[split_key])
            except ValueError as e:
                raise PartitionPlanError(
                    f"PoiskKino {endpoint}: {part} has {total} records > {max_records} "
                    "and cannot be split further"
                ) from e
            left_part = {**part, split_key: left}
            right_part = {**part, split_key: right}
            left_total = count(left_part)
            right_total = max(total - left_total, 0)
            logger.info(
                "PoiskKino %s derived %s: %d records", endpoint, right_part, right_total
            )
            pending[0:0] = [(left_part, left_total), (right_part, right_total)]
        return planned, used
    finally:
        if own_session:
            client.close()
