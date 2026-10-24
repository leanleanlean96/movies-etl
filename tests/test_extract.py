import gzip
import hashlib
import json

import pytest

from movies_pipeline.extract import MANIFEST_VERSION, extract_to_s3


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, *, Bucket, Key, Body, ContentType):
        self.objects[(Bucket, Key)] = Body


def page(*ids):
    return {"docs": [{"id": i} for i in ids]}


LOW = {"votes.kp": "1000-1999"}
HIGH = {"votes.kp": "2000-4999"}
EMPTY = {"votes.kp": "5000-19999"}


def run(pages):
    s3 = FakeS3()
    landed = extract_to_s3(
        pages=pages,
        s3=s3,
        bucket="etl-raw",
        prefix="/kinopoisk/movies/dt=2026-10-22/run=B1/",
        batch_id="B1",
        meta={"source": "poiskkino"},
    )
    key = "kinopoisk/movies/dt=2026-10-22/run=B1/_manifest.json"
    manifest = json.loads(s3.objects[("etl-raw", key)])
    return s3, landed, manifest


def test_manifest_counts_and_checksums_match_stored_pages():
    s3, landed, manifest = run(
        [(LOW, page(1, 2)), (LOW, page(3)), (HIGH, page(4, 5, 6)), (EMPTY, page())]
    )

    assert manifest["manifest_version"] == MANIFEST_VERSION
    assert manifest["status"] == "complete"
    assert manifest["batch_id"] == "B1"
    assert manifest["page_count"] == 3
    assert manifest["empty_pages"] == 1
    assert manifest["records"] == 6
    assert sum(p["records"] for p in manifest["pages"]) == manifest["records"]

    for entry in manifest["pages"]:
        body = s3.objects[("etl-raw", entry["key"])]
        assert entry["bytes"] == len(body)
        assert entry["sha256"] == hashlib.sha256(body).hexdigest()
        lines = gzip.decompress(body).decode().splitlines()
        assert len(lines) == entry["records"]

    assert landed == {
        "manifest_uri": "s3://etl-raw/kinopoisk/movies/dt=2026-10-22/run=B1/_manifest.json",
        "batch_id": "B1",
        "page_count": 3,
        "records": 6,
    }


def test_partition_stats_include_empty_partition():
    _, _, manifest = run(
        [(LOW, page(1, 2)), (LOW, page(3)), (HIGH, page(4)), (EMPTY, page())]
    )

    assert manifest["partition_stats"] == [
        {"partition": LOW, "pages": 2, "empty_pages": 0, "records": 3},
        {"partition": HIGH, "pages": 1, "empty_pages": 0, "records": 1},
        {"partition": EMPTY, "pages": 0, "empty_pages": 1, "records": 0},
    ]
    assert [p["partition"] for p in manifest["pages"]] == [LOW, LOW, HIGH]


def test_manifest_is_written_last():
    s3, _, _ = run([(LOW, page(1)), (HIGH, page(2))])

    assert list(s3.objects)[-1][1].endswith("/_manifest.json")


def test_empty_batch_gets_valid_manifest():
    s3, landed, manifest = run([(EMPTY, page())])

    assert len(s3.objects) == 1
    assert manifest["page_count"] == 0
    assert manifest["records"] == 0
    assert manifest["pages"] == []
    assert landed["records"] == 0


def test_failure_mid_batch_leaves_no_manifest():
    def pages():
        yield LOW, page(1)
        raise RuntimeError("API died")

    s3 = FakeS3()
    with pytest.raises(RuntimeError):
        extract_to_s3(
            pages=pages(), s3=s3, bucket="etl-raw", prefix="p", batch_id="B1", meta={}
        )

    assert not any(key.endswith("_manifest.json") for _, key in s3.objects)


def test_cyrillic_is_stored_unescaped():
    s3, _, manifest = run([(LOW, {"docs": [{"id": 1, "name": "Брат"}]})])

    body = s3.objects[("etl-raw", manifest["pages"][0]["key"])]
    line = gzip.decompress(body).decode("utf-8")
    assert '"name":"Брат"' in line
    assert "\\u" not in line


@pytest.mark.parametrize(
    "kwargs, message",
    [({"prefix": "/"}, "prefix"), ({"batch_id": ""}, "batch_id")],
)
def test_rejects_empty_prefix_or_batch_id(kwargs, message):
    params = {"prefix": "p", "batch_id": "B1", **kwargs}
    with pytest.raises(ValueError, match=message):
        extract_to_s3(pages=[], s3=FakeS3(), bucket="etl-raw", meta={}, **params)
