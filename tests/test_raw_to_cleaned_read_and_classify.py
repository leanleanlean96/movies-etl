import gzip
import json
from datetime import UTC, datetime

import pytest
from kinopoisk_cleaned.exceptions import (
    PageIntegrityError,
)
from kinopoisk_cleaned.manifests import (
    reconcile_page_counts,
)
from kinopoisk_cleaned.schema import (
    SCHEMA,
)
from kinopoisk_cleaned.transformations import (
    classify,
    read_pages,
)
from pyspark.sql.types import IntegerType, StringType


def write_page(root, key, lines):
    path = root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode()))
    return f"file://{path}"


def test_read_pages_preserves_origin_and_corrupt_rows(spark, tmp_path):
    first_key = "kinopoisk/dt=2026-10-25/run=example batch/page-00002.jsonl.gz"
    second_key = "kinopoisk/dt=2026-10-25/run=example batch/page-00001.jsonl.gz"
    first_uri = write_page(
        tmp_path,
        first_key,
        [json.dumps({"id": 10, "type": "movie"}), '{"id":'],
    )
    second_uri = write_page(
        tmp_path, second_key, [json.dumps({"id": 20, "type": "movie"})]
    )
    manifest = {
        "records": 3,
        "pages": [
            {"key": first_key, "records": 2},
            {"key": second_key, "records": 1},
        ],
    }
    pages = read_pages(spark, [second_uri, first_uri], SCHEMA, manifest=manifest)
    try:
        corrupt = pages.where("_corrupt_record IS NOT NULL").collect()
        assert len(corrupt) == 1
        assert corrupt[0].source_key == first_key
        assert corrupt[0].source_page == 1
        by_id = {row.id: row for row in pages.where("id IS NOT NULL").collect()}
        assert (by_id[10].source_key, by_id[10].source_page) == (first_key, 1)
        assert (by_id[20].source_key, by_id[20].source_page) == (second_key, 2)
        assert pages.is_cached
        assert pages.schema["source_key"].dataType == StringType()
        assert pages.schema["source_page"].dataType == IntegerType()
        counts = {
            row.source_key: row["count"]
            for row in pages.groupBy("source_key").count().collect()
        }
        reconcile_page_counts(manifest, counts)
    finally:
        pages.unpersist()


def test_read_pages_handles_empty_batch(spark):
    pages = read_pages(spark, [], SCHEMA, manifest={"pages": []})
    try:
        assert pages.count() == 0
        assert pages.schema["source_key"].dataType == StringType()
        assert pages.schema["source_page"].dataType == IntegerType()
    finally:
        pages.unpersist()


@pytest.mark.parametrize(
    "keys",
    [[], ["page.jsonl.gz", "nested/page.jsonl.gz"]],
    ids=["unlisted-file", "ambiguous-file"],
)
def test_read_pages_rejects_file_without_unique_manifest_page(spark, tmp_path, keys):
    uri = write_page(tmp_path, "nested/page.jsonl.gz", ['{"id": 1}'])
    with pytest.raises(PageIntegrityError, match="exactly one matching manifest page"):
        read_pages(
            spark, [uri], SCHEMA, manifest={"pages": [{"key": key} for key in keys]}
        )


@pytest.mark.parametrize("expected_type", ["movie", None])
def test_classify_checks_null_type_even_without_expected_type(spark, expected_type):
    df = spark.createDataFrame(
        [
            (1, None, "2026-10-25T12:00:00Z", None),
            (2, "movie", "2026-10-25T12:00:00Z", None),
            (3, "tv-series", "2026-10-25T12:00:00Z", None),
        ],
        "id long, type string, updatedAt string, _corrupt_record string",
    )
    checked = classify(
        spark,
        df,
        expected_type,
        datetime(2026, 10, 25, tzinfo=UTC),
        datetime(2026, 10, 26, tzinfo=UTC),
    )
    actual = {row.id: row.reason for row in checked.collect()}
    assert actual == {
        1: "unexpected_type",
        2: None,
        3: "unexpected_type" if expected_type == "movie" else None,
    }
