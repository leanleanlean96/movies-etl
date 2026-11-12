from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

from .constants import DEFAULT_OUTPUT_BUCKET, TABLE_SORT_KEYS, TABLE_STATS
from .manifests import (
    build_cleaned_manifest,
    read_manifest,
    reconcile_page_counts,
    verify_pages,
)
from .paths import cleaned_manifest_uri, cleaned_table_uri, page_uris
from .schema import SCHEMA
from .statistics import collect_stats
from .storage import delete_path, verify_table, write_json, write_table
from .transformations import (
    classify,
    deduplicate,
    quarantine,
    read_pages,
    to_countries,
    to_genres,
    to_movies,
)


def transform_tables(
    spark: SparkSession, pages: DataFrame, manifest: Mapping[str, Any]
) -> tuple[dict[str, DataFrame], dict[str, int]]:
    batch_id = manifest["batch_id"]
    interval_start = datetime.fromisoformat(
        manifest["interval_start"].replace("Z", "+00:00")
    )
    interval_end = datetime.fromisoformat(
        manifest["interval_end"].replace("Z", "+00:00")
    )
    checked = classify(
        spark, pages, manifest["params"].get("type"), interval_start, interval_end
    )
    valid = checked.where(col("reason").isNull())
    deduplicated, duplicates = deduplicate(spark, valid)
    tables = {
        "quarantine": quarantine(spark, checked, batch_id),
        "movies": to_movies(
            spark, deduplicated, batch_id=batch_id, ingested_at=manifest["finished_at"]
        ),
        "movie_genres": to_genres(spark, deduplicated, batch_id),
        "movie_countries": to_countries(spark, deduplicated, batch_id),
    }
    stats = collect_stats(checked, tables, duplicates, manifest["records"])
    return tables, stats


def write_tables(
    spark: SparkSession,
    tables: Mapping[str, DataFrame],
    stats: Mapping[str, int],
    batch_id: str,
    output_bucket: str,
    output_root: str | None,
) -> dict[str, int]:
    table_rows = {}
    for table, df in tables.items():
        uri = cleaned_table_uri(output_bucket, table, batch_id, output_root)
        write_table(df, uri, TABLE_SORT_KEYS[table])
        table_rows[table] = verify_table(spark, uri, df, stats[TABLE_STATS[table]])
    return table_rows


def run(
    spark: SparkSession,
    manifest_uri: str,
    *,
    output_bucket: str = DEFAULT_OUTPUT_BUCKET,
    input_root: str | None = None,
    output_root: str | None = None,
) -> dict[str, Any]:
    pages = None
    started_at = datetime.now(timezone.utc)
    try:
        manifest = read_manifest(spark, manifest_uri)
        batch_id = manifest["batch_id"]
        output_manifest_uri = cleaned_manifest_uri(output_bucket, batch_id, output_root)
        delete_path(spark, output_manifest_uri)
        verify_pages(spark, manifest, input_root)
        pages = read_pages(
            spark, page_uris(manifest, input_root), SCHEMA, manifest=manifest
        )
        page_counts = {
            row["source_key"]: row["count"]
            for row in pages.groupBy("source_key").count().collect()
        }
        reconcile_page_counts(manifest, page_counts)
        tables, stats = transform_tables(spark, pages, manifest)
        table_rows = write_tables(
            spark, tables, stats, batch_id, output_bucket, output_root
        )
        cleaned = build_cleaned_manifest(
            raw_manifest=manifest,
            raw_manifest_uri=manifest_uri,
            output_bucket=output_bucket,
            output_root=output_root,
            stats=stats,
            table_rows=table_rows,
            table_schemas={name: df.schema for name, df in tables.items()},
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
        )
        try:
            write_json(spark, output_manifest_uri, cleaned)
        except BaseException:
            delete_path(spark, output_manifest_uri)
            raise
        return cleaned
    finally:
        if pages is not None:
            pages.unpersist()
