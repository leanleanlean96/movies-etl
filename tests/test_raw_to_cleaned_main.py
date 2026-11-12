import gzip
import hashlib
import json
from pathlib import Path
from unittest.mock import Mock
from urllib.parse import urlparse

import pytest
from kinopoisk_cleaned import pipeline as job
from kinopoisk_cleaned.constants import QUARANTINE_REASONS
from kinopoisk_cleaned.exceptions import PageIntegrityError, ReconciliationError
from kinopoisk_cleaned.statistics import collect_stats
from kinopoisk_cleaned.storage import write_bytes
from pyspark.sql.functions import col
from pyspark.sql.types import StructType


@pytest.fixture
def main_batch(spark, tmp_path, monkeypatch):
    key = "kinopoisk/batch/page-00001.jsonl.gz"
    path = tmp_path / key
    path.parent.mkdir(parents=True)
    movie = {
        "id": 1,
        "type": "movie",
        "updatedAt": "2026-10-25T12:00:00Z",
        "genres": [{"id": 10, "name": "Drama"}],
        "countries": [{"id": 20, "name": "France"}],
    }
    rows = [
        {**movie, "updatedAt": "2026-10-25T10:00:00Z"},
        movie,
        {**movie, "id": 2, "genres": [], "countries": []},
        '{"id":',
        {**movie, "id": None},
        {**movie, "updatedAt": "bad"},
        {**movie, "updatedAt": "2026-10-26T00:00:00Z"},
        {**movie, "type": "tv-series"},
    ]
    data = gzip.compress(
        (
            "\n".join(row if isinstance(row, str) else json.dumps(row) for row in rows)
            + "\n"
        ).encode()
    )
    path.write_bytes(data)
    manifest = {
        "manifest_version": 2,
        "status": "complete",
        "batch_id": "check",
        "bucket": "etl-raw",
        "prefix": "kinopoisk/batch",
        "checksum": {"algorithm": "sha256"},
        "records": len(rows),
        "page_count": 1,
        "pages": [
            {
                "key": key,
                "records": len(rows),
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        ],
        "params": {"type": "movie"},
        "interval_start": "2026-10-25T00:00:00Z",
        "interval_end": "2026-10-26T00:00:00Z",
        "finished_at": "2026-10-26T01:00:00Z",
    }
    manifest_path = tmp_path / "_manifest.json"
    output_root = (tmp_path / "cleaned").as_uri()

    def run():
        manifest_path.write_text(json.dumps(manifest))
        cleaned = job.run(
            spark,
            manifest_path.as_uri(),
            input_root=tmp_path.as_uri(),
            output_root=output_root,
        )
        print(json.dumps(cleaned["stats"], sort_keys=True))

    run.output_root = output_root
    run.manifest_path = (
        tmp_path / "cleaned/kinopoisk/_manifests/batch_id=check/_manifest.json"
    )
    return manifest, run


@pytest.mark.parametrize("empty", [False, True], ids=["mixed", "empty"])
def test_main_reports_verified_statistics(main_batch, capsys, empty, spark):
    manifest, run = main_batch
    if empty:
        manifest.update(pages=[], page_count=0, records=0)
    run()
    stats = json.loads(capsys.readouterr().out)
    assert stats == {
        "read": 0 if empty else 8,
        "quarantined": 0 if empty else 5,
        "duplicates": 0 if empty else 1,
        "movies": 0 if empty else 2,
        "genre_links": 0 if empty else 1,
        "country_links": 0 if empty else 1,
        **{f"quarantined_{reason}": 0 if empty else 1 for reason in QUARANTINE_REASONS},
    }
    cleaned = json.loads(run.manifest_path.read_text())
    assert cleaned["stats"] == stats
    assert set(cleaned["tables"]) == set(job.TABLE_STATS)
    assert cleaned["source"]["interval_start"] == manifest["interval_start"]
    assert cleaned["source"]["type"] == "movie"
    assert all(
        table["uri"].startswith(run.output_root) for table in cleaned["tables"].values()
    )
    for table in cleaned["tables"].values():
        declared = StructType.fromJson(table["schema"])
        written = (
            spark.read.option("recursiveFileLookup", "true")
            .parquet(table["uri"])
            .schema
        )
        assert [(f.name, f.dataType) for f in declared] == [
            (f.name, f.dataType) for f in written
        ]


@pytest.mark.parametrize("field", ["bytes", "sha256", "records"])
def test_main_rejects_corrupt_or_miscounted_page(main_batch, capsys, field):
    manifest, run = main_batch
    if field == "sha256":
        manifest["pages"][0][field] = "0" * 64
    else:
        manifest["pages"][0][field] += 1
        if field == "records":
            manifest["records"] += 1
    expected = ReconciliationError if field == "records" else PageIntegrityError
    with pytest.raises(expected):
        run()
    assert not capsys.readouterr().out


def test_collect_stats_rejects_unknown_reason():
    checked = Mock()
    checked.groupBy.return_value.count.return_value.collect.return_value = [
        {"reason": "unknown", "count": 1}
    ]
    with pytest.raises(ReconciliationError, match="unknown quarantine reasons"):
        collect_stats(checked, {}, 0, 1)


def test_main_repeat_preserves_contents_and_other_batches(main_batch, spark, capsys):
    _, run = main_batch
    other_batch = (
        Path(urlparse(run.output_root).path) / "kinopoisk/movies/batch_id=other"
    )
    other_batch.mkdir(parents=True)
    sentinel = other_batch / "keep.txt"
    sentinel.write_text("keep")

    def contents():
        cleaned = json.loads(run.manifest_path.read_text())
        return {
            name: sorted(
                repr(row.asDict())
                for row in spark.read.option("recursiveFileLookup", "true")
                .parquet(table["uri"])
                .collect()
            )
            for name, table in cleaned["tables"].items()
        }

    run()
    first_stats = json.loads(capsys.readouterr().out)
    first_contents = contents()
    run()
    assert json.loads(capsys.readouterr().out) == first_stats
    assert contents() == first_contents
    assert sentinel.read_text() == "keep"


@pytest.mark.parametrize("failure", ["checksum", "second-table", "manifest"])
def test_failed_repeat_removes_completion_marker(
    main_batch, spark, monkeypatch, capsys, failure
):
    manifest, run = main_batch
    run()
    capsys.readouterr()
    assert run.manifest_path.exists()
    if failure == "checksum":
        manifest["pages"][0]["sha256"] = "0" * 64
        expected = PageIntegrityError
    elif failure == "second-table":
        original = job.write_table
        calls = []

        def fail_second(df, uri, keys):
            calls.append(uri)
            if len(calls) == 2:
                raise OSError("write failed")
            original(df, uri, keys)

        monkeypatch.setattr(job, "write_table", fail_second)
        expected = OSError
    else:

        def fail_manifest(spark, uri, value):
            write_bytes(spark, uri, b"{")
            raise OSError("manifest failed")

        monkeypatch.setattr(job, "write_json", fail_manifest)
        expected = OSError
    with pytest.raises(expected):
        run()
    assert not run.manifest_path.exists()
    assert not capsys.readouterr().out


@pytest.mark.parametrize("mismatch", ["count", "schema"])
def test_verify_table_rejects_mismatch(spark, tmp_path, mismatch):
    df = spark.createDataFrame([(1,)], "id long")
    uri = (tmp_path / "table").as_uri()
    job.write_table(df, uri, ["id"])
    expected = df if mismatch == "count" else df.selectExpr("cast(id as string) as id")
    with pytest.raises(ReconciliationError, match="rows|schema"):
        job.verify_table(spark, uri, expected, 2 if mismatch == "count" else 1)


def test_deduplicate_breaks_ties_within_page(spark):
    df = spark.createDataFrame(
        [(1, "2026-10-25 12:00:00", 1, "A"), (1, "2026-10-25 12:00:00", 1, "Z")],
        "id long, source_updated_at string, source_page int, name string",
    )
    first, duplicates = job.deduplicate(spark, df)
    second, _ = job.deduplicate(spark, df.orderBy(col("name").desc()))
    assert duplicates == 1
    assert first.collect() == second.collect()
