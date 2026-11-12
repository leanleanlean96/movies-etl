from copy import deepcopy

import pytest

from movies_pipeline.replay import manifest_location, replay_batch

URI = "s3://etl-raw/kinopoisk/movies/dt=2026-11-11/run=20261112T042321Z/_manifest.json"


@pytest.fixture
def manifest():
    return {
        "status": "complete",
        "bucket": "etl-raw",
        "prefix": URI.removeprefix("s3://etl-raw/").rsplit("/", 1)[0],
        "batch_id": "20261112T042321Z",
        "interval_start": "2026-11-11T00:00:00Z",
        "interval_end": "2026-11-12T00:00:00Z",
        "finished_at": "2026-11-12T04:25:00Z",
    }


def test_replay_preserves_identity_and_observation_time(manifest):
    original = deepcopy(manifest)
    result = replay_batch(URI, manifest)
    assert result == {
        "manifest_uri": URI,
        "batch_id": original["batch_id"],
        "interval_start": original["interval_start"],
        "interval_end": original["interval_end"],
        "ingested_at": original["finished_at"],
    }
    assert manifest == original
    assert replay_batch(URI, manifest) == result


@pytest.mark.parametrize(
    "uri",
    [
        None,
        "",
        "https://host/_manifest.json",
        URI + "?x=1",
        URI + "#fragment",
        URI.replace("etl-raw", "etl-cleaned"),
        URI.replace("_manifest.json", "page.jsonl.gz"),
    ],
)
def test_rejects_invalid_location(uri):
    with pytest.raises((TypeError, ValueError)):
        manifest_location(uri)


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "incomplete"),
        ("batch_id", "../other"),
        ("batch_id", ""),
        ("bucket", "other"),
        ("prefix", "other"),
        ("interval_start", "2026-11-12T00:00:00Z"),
        ("interval_end", "2026-11-10T00:00:00Z"),
        ("finished_at", "2026-11-12T04:25:00"),
        ("finished_at", None),
    ],
)
def test_rejects_invalid_identity_or_interval(manifest, field, value):
    manifest[field] = value
    with pytest.raises(ValueError):
        replay_batch(URI, manifest)
