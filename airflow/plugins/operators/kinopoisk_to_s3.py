import math
from collections.abc import Collection, Mapping, Sequence
from datetime import UTC, datetime
from itertools import pairwise
from typing import Any, ClassVar

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import BaseOperator

from movies_pipeline.exceptions import MaxPagesBudgetExceeded
from movies_pipeline.extract import extract_to_s3
from movies_pipeline.interval import updated_at_filter
from movies_pipeline.sources.kinopoisk import (
    FREE_PLAN_MAX_PAGES_PER_QUERY,
    build_kinopoisk_session,
    iter_partitioned_pages,
    parse_range,
    plan_partitions,
)

RESERVED_API_PARAMS = frozenset({"limit", "updatedAt", "next", "prev", "withCount"})


def parse_bound(value: datetime | str, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(value.strip())
        except ValueError as e:
            raise ValueError(f"{name}={value!r} is not an ISO date or datetime") from e
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def pages_needed(planned: Sequence[Mapping[str, Any]], page_limit: int) -> int:
    return sum(max(1, math.ceil(item["total"] / page_limit)) for item in planned)


class KinopoiskToS3Operator(BaseOperator):
    template_fields: Collection[str] = (
        "endpoint",
        "api_params",
        "partitions",
        "s3_bucket",
        "s3_prefix",
        "interval_start",
        "interval_end",
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        "api_params": "json",
        "partitions": "json",
    }

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_prefix: str,
        endpoint: str = "/movie",
        api_params: Mapping[str, Any] | None = None,
        partitions: Sequence[Mapping[str, Any]] | None = None,
        split_key: str | None = None,
        partition_fill_ratio: float = 0.9,
        max_count_requests: int = 30,
        page_limit: int = 250,
        max_pages: int = 60,
        max_pages_per_query: int = FREE_PLAN_MAX_PAGES_PER_QUERY,
        aws_conn_id: str = "minio_s3",
        interval_start: datetime | str | None = None,
        interval_end: datetime | str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if (interval_start is None) != (interval_end is None):
            raise ValueError("interval_start and interval_end must be set together")
        if not 0 < partition_fill_ratio <= 1:
            raise ValueError("partition_fill_ratio must be in (0, 1]")
        self.endpoint = endpoint
        self.api_params = dict(api_params or {})
        self.partitions = [dict(part) for part in partitions or []]
        self.split_key = split_key
        self.partition_fill_ratio = partition_fill_ratio
        self.max_count_requests = max_count_requests
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.page_limit = page_limit
        self.max_pages = max_pages
        self.max_pages_per_query = max_pages_per_query
        self.aws_conn_id = aws_conn_id
        self.interval_start = interval_start
        self.interval_end = interval_end

    @property
    def max_records_per_query(self) -> int:
        return max(
            1,
            math.floor(
                self.page_limit * self.max_pages_per_query * self.partition_fill_ratio
            ),
        )

    def resolve_interval(self, context: Any) -> tuple[datetime, datetime, str]:
        if self.interval_start is not None and self.interval_end is not None:
            return (
                parse_bound(self.interval_start, "interval_start"),
                parse_bound(self.interval_end, "interval_end"),
                "explicit",
            )
        start = context.get("data_interval_start")
        end = context.get("data_interval_end")
        if start is None or end is None:
            raise ValueError(
                "data interval is not set: pass interval_start/interval_end or run the DAG on schedule"
            )
        return start, end, "data_interval"

    def validate_params(self) -> None:
        reserved = RESERVED_API_PARAMS & set(self.api_params)
        if reserved:
            raise ValueError(
                f"api_params must not contain {sorted(reserved)}: they are set by the operator"
            )
        for part in self.partitions:
            reserved = RESERVED_API_PARAMS & set(part)
            if reserved:
                raise ValueError(
                    f"partition {part} must not contain {sorted(reserved)}"
                )
            overlap = set(part) & set(self.api_params)
            if overlap:
                raise ValueError(
                    f"partition {part} repeats api_params keys {sorted(overlap)}: "
                    "keep common filters in api_params and the splitting key only in partitions"
                )
        if len({tuple(sorted(part.items())) for part in self.partitions}) != len(
            self.partitions
        ):
            raise ValueError("partitions contain duplicates")
        if self.split_key is None:
            return
        if self.split_key in self.api_params:
            raise ValueError(f"split_key {self.split_key!r} must not be in api_params")
        ranges = []
        for part in self.partitions:
            if self.split_key not in part:
                raise ValueError(
                    f"partition {part} has no split_key {self.split_key!r}"
                )
            ranges.append(parse_range(part[self.split_key]))
        ranges.sort()
        for (_, prev_high), (low, _) in pairwise(ranges):
            if low <= prev_high:
                raise ValueError(f"partitions overlap on {self.split_key!r}")

    def plan(
        self, query: Mapping[str, Any], session: Any
    ) -> tuple[list[dict[str, Any]], int]:
        if self.split_key is None:
            return [
                {"partition": part, "total": None} for part in self.partitions or [{}]
            ], 0
        planned, count_requests = plan_partitions(
            self.endpoint,
            query,
            self.partitions,
            split_key=self.split_key,
            max_records=self.max_records_per_query,
            max_count_requests=self.max_count_requests,
            session=session,
        )
        needed = pages_needed(planned, self.page_limit)
        self.log.info(
            "Partition plan: %d partition(s), %d records, %d pages, %d count requests",
            len(planned),
            sum(item["total"] for item in planned),
            needed,
            count_requests,
        )
        if needed > self.max_pages:
            raise MaxPagesBudgetExceeded(
                f"PoiskKino {self.endpoint}: plan needs {needed} pages > max_pages={self.max_pages}; "
                "raise max_pages"
            )
        return planned, count_requests

    def execute(self, context: Any) -> dict[str, Any]:
        start, end, interval_source = self.resolve_interval(context)

        self.validate_params()
        query = {
            **self.api_params,
            "limit": self.page_limit,
            "updatedAt": updated_at_filter(start, end),
        }
        batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        prefix = f"{self.s3_prefix.strip('/')}/run={batch_id}"

        self.log.info(
            "Extracting %s with %s, %d seed partition(s) (interval from %s) into s3://%s/%s "
            "(max_pages=%d, max_pages_per_query=%d, split_key=%s)",
            self.endpoint,
            query,
            len(self.partitions) or 1,
            interval_source,
            self.s3_bucket,
            prefix,
            self.max_pages,
            self.max_pages_per_query,
            self.split_key,
        )

        session = build_kinopoisk_session()
        try:
            planned, count_requests = self.plan(query, session)
            s3 = S3Hook(aws_conn_id=self.aws_conn_id).get_conn()
            return extract_to_s3(
                pages=iter_partitioned_pages(
                    self.endpoint,
                    query,
                    [item["partition"] for item in planned],
                    max_pages_total=self.max_pages,
                    max_pages_per_query=self.max_pages_per_query,
                    session=session,
                ),
                s3=s3,
                bucket=self.s3_bucket,
                prefix=prefix,
                batch_id=batch_id,
                meta={
                    "source": "poiskkino",
                    "endpoint": self.endpoint,
                    "params": query,
                    "partitions": self.partitions,
                    "split_key": self.split_key,
                    "partition_plan": planned,
                    "count_requests": count_requests,
                    "interval_start": start.isoformat(),
                    "interval_end": end.isoformat(),
                    "interval_source": interval_source,
                    "dag_id": self.dag_id,
                    "task_id": self.task_id,
                    "run_id": context.get("run_id"),
                    "try_number": getattr(context.get("ti"), "try_number", None),
                },
            )
        finally:
            session.close()
