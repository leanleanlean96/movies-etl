import copy
import gzip
import hashlib
import json
from datetime import UTC, datetime

import pytest
from kinopoisk_cleaned.exceptions import (
    ManifestError,
    PageIntegrityError,
    ReconciliationError,
)
from kinopoisk_cleaned.manifests import (
    build_cleaned_manifest,
    check_manifest,
    check_page_bytes,
    read_manifest,
    reconcile_page_counts,
)
from kinopoisk_cleaned.paths import (
    cleaned_manifest_uri,
    cleaned_table_uri,
    object_key,
    page_numbers,
    page_uris,
)
from kinopoisk_cleaned.statistics import (
    reconcile_totals,
)
from kinopoisk_cleaned.storage import (
    read_bytes,
    write_bytes,
    write_json,
)
from pyspark.sql.types import LongType, StructField, StructType

from movies_pipeline.extract import extract_to_s3

PREFIX = "kinopoisk/movies/dt=2026-10-23/run=20261024T210922Z"
KEY_1 = f"{PREFIX}/page-00001.jsonl.gz"
KEY_2 = f"{PREFIX}/page-00002.jsonl.gz"


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, *, Bucket, Key, Body, ContentType):
        self.objects[(Bucket, Key)] = Body


def landed_manifest():
    s3 = FakeS3()
    landed = extract_to_s3(
        pages=[
            ({"votes.kp": "1000-1999"}, {"docs": [{"id": 1}, {"id": 2}]}),
            ({"votes.kp": "1000-1999"}, {"docs": []}),
            ({"votes.kp": "2000-4999"}, {"docs": [{"id": 3}]}),
        ],
        s3=s3,
        bucket="etl-raw",
        prefix=PREFIX,
        batch_id="20261024T210922Z",
        meta={
            "source": "kinopoisk",
            "interval_start": "2026-10-23T00:00:00Z",
            "interval_end": "2026-10-24T00:00:00Z",
            "params": {"type": "movie"},
        },
    )
    key = landed["manifest_uri"].removeprefix("s3://etl-raw/")
    return json.loads(s3.objects[("etl-raw", key)]), s3


@pytest.fixture
def manifest():
    return landed_manifest()[0]


def test_manifest_written_by_extract_passes(manifest):
    check_manifest(manifest)
    assert manifest["page_count"] == 2
    assert manifest["records"] == 3


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda m: m.update(manifest_version=1), "manifest_version must be 2"),
        (lambda m: m.update(status="partial"), "status must be 'complete'"),
        (lambda m: m.update(page_count=5), "page_count=5 but 2 pages listed"),
        (lambda m: m.update(records=99), "records=99 but pages sum to 3"),
        (lambda m: m.update(bucket=""), "bucket is missing"),
        (lambda m: m.pop("checksum"), "checksum.algorithm must be 'sha256'"),
        (lambda m: m["pages"][0].update(records=True), "records must be a positive"),
        (lambda m: m["pages"][0].update(records=0), "records must be a positive"),
        (lambda m: m["pages"][0].update(bytes=0), "bytes must be a positive"),
        (lambda m: m["pages"][0].update(sha256="xyz"), "is not a hex digest"),
        (lambda m: m["pages"][0].update(key="other/p.jsonl.gz"), "outside prefix"),
        (lambda m: m["pages"][1].update(key=m["pages"][0]["key"]), "duplicate page"),
        (lambda m: m.update(pages=None), "pages is not a list"),
    ],
)
def test_broken_manifest_is_rejected(manifest, mutate, message):
    broken = copy.deepcopy(manifest)
    mutate(broken)
    with pytest.raises(ManifestError, match=message):
        check_manifest(broken)


def test_all_problems_are_reported_at_once(manifest):
    broken = copy.deepcopy(manifest)
    broken.update(manifest_version=1, status=None, page_count=7)
    with pytest.raises(ManifestError) as err:
        check_manifest(broken)
    assert str(err.value).count(";") == 2


def test_not_a_dict_is_rejected():
    with pytest.raises(ManifestError, match="not a JSON object"):
        check_manifest([])


def test_empty_batch_is_structurally_valid(manifest):
    empty = copy.deepcopy(manifest)
    empty.update(pages=[], page_count=0, records=0)
    check_manifest(empty)


def test_page_uris_keep_manifest_order(manifest):
    assert page_uris(manifest) == [f"s3a://etl-raw/{KEY_1}", f"s3a://etl-raw/{KEY_2}"]


def test_page_numbers_start_from_one(manifest):
    assert page_numbers(manifest) == {KEY_1: 1, KEY_2: 2}


@pytest.mark.parametrize(
    "value",
    [
        f"s3a://etl-raw/{KEY_1}",
        f"s3://etl-raw/{KEY_1}",
        f"s3a://etl-raw/{KEY_1.replace('=', '%3D')}",
        KEY_1,
        f"/{KEY_1}",
    ],
)
def test_object_key_normalizes_uri_forms(value):
    assert object_key(value) == KEY_1


def test_object_key_rejects_other_schemes():
    with pytest.raises(ValueError, match="not an S3 URI"):
        object_key(f"file:///{KEY_1}")


def test_page_bytes_match_manifest():
    manifest, s3 = landed_manifest()
    for page in manifest["pages"]:
        assert check_page_bytes(page, s3.objects[("etl-raw", page["key"])]) == []


def test_page_bytes_mismatch_is_reported(manifest):
    page = manifest["pages"][0]
    other = gzip.compress(b'{"id":42}\n')
    problems = check_page_bytes(page, other)
    assert any("bytes, manifest says" in p for p in problems)
    assert any("sha256 mismatch" in p for p in problems)


def test_same_size_different_content_is_caught(manifest):
    page = dict(manifest["pages"][0])
    data = b"x" * page["bytes"]
    assert check_page_bytes(page, data) == [f"{page['key']}: sha256 mismatch"]
    page["sha256"] = hashlib.sha256(data).hexdigest()
    assert check_page_bytes(page, data) == []


def test_page_counts_reconcile_with_full_uris(manifest):
    reconcile_page_counts(
        manifest, {f"s3a://etl-raw/{KEY_1}": 2, f"s3a://etl-raw/{KEY_2}": 1}
    )


def test_page_count_problems_are_listed(manifest):
    counts = {KEY_1: 1, f"{PREFIX}/page-00009.jsonl.gz": 5}
    with pytest.raises(ReconciliationError) as err:
        reconcile_page_counts(manifest, counts)
    message = str(err.value)
    assert f"{KEY_2}: no rows read, manifest says 1" in message
    assert "page-00009.jsonl.gz: 5 rows read from a file not in manifest" in message
    assert f"{KEY_1}: 1 rows read, manifest says 2" in message
    assert "total: 6 rows read, manifest says 3" in message


def test_totals_reconcile():
    reconcile_totals({"read": 10, "movies": 7, "quarantined": 2, "duplicates": 1})


def test_totals_mismatch_is_reported():
    with pytest.raises(ReconciliationError, match="read=10 but"):
        reconcile_totals({"read": 10, "movies": 7, "quarantined": 2, "duplicates": 0})


def test_totals_require_all_counters():
    with pytest.raises(ReconciliationError, match="missing"):
        reconcile_totals({"read": 10, "movies": 10})


def test_cleaned_paths():
    assert (
        cleaned_table_uri("etl-cleaned", "movie_genre", "20261024T210922Z")
        == "s3a://etl-cleaned/kinopoisk/movie_genre/batch_id=20261024T210922Z"
    )
    assert cleaned_manifest_uri("etl-cleaned", "20261024T210922Z") == (
        "s3a://etl-cleaned/kinopoisk/_manifests/batch_id=20261024T210922Z/_manifest.json"
    )


@pytest.mark.parametrize("table", ["Movies", "movie-genre", "1movies", ""])
def test_bad_table_name_is_rejected(table):
    with pytest.raises(ValueError, match="bad table name"):
        cleaned_table_uri("etl-cleaned", table, "b1")


@pytest.mark.parametrize("batch_id", ["", "a/b", " b1"])
def test_bad_batch_id_is_rejected(batch_id):
    with pytest.raises(ValueError, match="bad batch_id"):
        cleaned_table_uri("etl-cleaned", "movies", batch_id)


def cleaned(manifest, **overrides):
    kwargs = {
        "raw_manifest": manifest,
        "raw_manifest_uri": f"s3a://etl-raw/{PREFIX}/_manifest.json",
        "output_bucket": "etl-cleaned",
        "stats": {
            "read": 3,
            "movies": 2,
            "quarantined": 0,
            "duplicates": 1,
            "genre_links": 5,
            "country_links": 3,
        },
        "table_rows": {
            "movies": 2,
            "movie_genres": 5,
            "movie_countries": 3,
            "quarantine": 0,
        },
        "table_schemas": {
            name: StructType([StructField("id", LongType())])
            for name in ("movies", "movie_genres", "movie_countries", "quarantine")
        },
        "started_at": datetime(2026, 10, 25, 1, 0, tzinfo=UTC),
        "finished_at": datetime(2026, 10, 25, 1, 5, tzinfo=UTC),
    }
    kwargs.update(overrides)
    return build_cleaned_manifest(**kwargs)


def test_cleaned_manifest_links_raw_batch(manifest):
    result = cleaned(manifest)
    assert result["batch_id"] == "20261024T210922Z"
    assert result["status"] == "complete"
    assert result["source"]["manifest_uri"].endswith("/_manifest.json")
    assert result["source"]["records"] == 3
    assert result["tables"]["movies"] == {
        "uri": "s3a://etl-cleaned/kinopoisk/movies/batch_id=20261024T210922Z",
        "rows": 2,
        "schema": StructType([StructField("id", LongType())]).jsonValue(),
    }
    json.dumps(result)


def test_cleaned_manifest_requires_all_schemas(manifest):
    with pytest.raises(ReconciliationError, match="four table schemas"):
        cleaned(manifest, table_schemas={})


def test_cleaned_manifest_refuses_unbalanced_stats(manifest):
    stats = {"read": 3, "movies": 3, "quarantined": 0, "duplicates": 1}
    with pytest.raises(ReconciliationError):
        cleaned(manifest, stats=stats, table_rows={"movies": 3})


def test_cleaned_manifest_refuses_movies_rows_mismatch(manifest):
    with pytest.raises(ReconciliationError, match="table movies has 1 rows"):
        cleaned(manifest, table_rows={"movies": 1})


def test_cleaned_manifest_requires_aware_datetimes(manifest):
    with pytest.raises(ValueError, match="timezone-aware"):
        cleaned(manifest, started_at=datetime(2026, 10, 25, 1, 0))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("interval_start", None),
        ("interval_start", "bad"),
        ("interval_start", "2026-10-23T00:00:00"),
        ("interval_end", "2026-10-22T00:00:00Z"),
        ("interval_end", "2026-10-23T00:00:00Z"),
        ("finished_at", None),
        ("finished_at", "2026-10-24T00:00:00"),
        ("params", None),
        ("params", []),
        ("params", {"type": 1}),
        ("batch_id", ".."),
        ("batch_id", "a/b"),
    ],
)
def test_manifest_rejects_bad_processing_metadata(manifest, field, value):
    manifest[field] = value
    with pytest.raises(ManifestError):
        check_manifest(manifest)


@pytest.mark.parametrize(
    "table", ["movies", "movie_genres", "movie_countries", "quarantine"]
)
def test_cleaned_manifest_rejects_any_table_count_mismatch(manifest, table):
    counts = {"movies": 2, "movie_genres": 5, "movie_countries": 3, "quarantine": 0}
    counts[table] += 1
    with pytest.raises(ReconciliationError, match=f"table {table}"):
        cleaned(manifest, table_rows=counts)


def test_hadoop_bytes_roundtrip_reads_underscore_files(spark, tmp_path):
    uri = (tmp_path / "_manifest.json").as_uri()
    write_bytes(spark, uri, b"\x00\x01binary\xff")
    assert read_bytes(spark, uri) == b"\x00\x01binary\xff"


def test_read_manifest_checks_what_it_reads(spark, tmp_path, manifest):
    good = (tmp_path / "good" / "_manifest.json").as_uri()
    write_json(spark, good, manifest)
    assert read_manifest(spark, good) == manifest

    broken = (tmp_path / "broken" / "_manifest.json").as_uri()
    write_json(spark, broken, {**manifest, "status": "partial"})
    with pytest.raises(ManifestError):
        read_manifest(spark, broken)


def test_verify_pages_reads_every_page_and_reports_bad_ones(monkeypatch):
    from kinopoisk_cleaned import manifests as job

    manifest, s3 = landed_manifest()
    stored = {f"s3a://etl-raw/{key}": body for (_, key), body in s3.objects.items()}
    read = []

    def fake_read_bytes(spark, uri):
        read.append(uri)
        return stored[uri]

    monkeypatch.setattr(job, "read_bytes", fake_read_bytes)
    job.verify_pages(None, manifest)
    assert read == page_uris(manifest)

    stored[f"s3a://etl-raw/{KEY_2}"] = b"tampered"
    with pytest.raises(PageIntegrityError, match="page-00002.jsonl.gz"):
        job.verify_pages(None, manifest)
