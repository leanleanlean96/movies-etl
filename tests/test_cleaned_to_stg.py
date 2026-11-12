import json
from unittest.mock import Mock

import kinopoisk_staging as job
import kinopoisk_stg_jdbc as jdbc
import pytest
from kinopoisk_stg_support import CloudberrySettings, StagingValidationError
from test_cleaned_to_stg_support import frames, manifest, target_columns


@pytest.fixture
def cleaned_batch(spark, tmp_path, monkeypatch):
    value = manifest()
    tables = frames(spark)
    for table, df in tables.items():
        path = tmp_path / table
        df.write.parquet(str(path))
        value["tables"][table]["schema"] = df.schema.jsonValue()
    # Quarantine is described by the manifest, but must never be loaded into stg.
    value["tables"]["quarantine"]["schema"] = tables["movies"].schema.jsonValue()
    manifest_uri = "s3://etl-cleaned/kinopoisk/_manifests/batch_id=demo/_manifest.json"
    manifest_path = tmp_path / "_manifest.json"
    manifest_path.write_text(json.dumps(value))
    paths = {
        entry["uri"]: (tmp_path / table).as_uri()
        for table, entry in value["tables"].items()
    }
    paths[manifest_uri] = manifest_path.as_uri()
    monkeypatch.setattr(job, "s3a_uri", paths.__getitem__)
    return value, manifest_path, manifest_uri, tables


def test_pipeline_reads_parquet_validates_and_releases_cache(
    spark, cleaned_batch, monkeypatch
):
    _, _, uri, tables = cleaned_batch
    db = Mock()
    monkeypatch.setattr(job, "JdbcConnection", Mock(return_value=db))
    metadata = {table: target_columns(df.schema) for table, df in tables.items()}
    monkeypatch.setattr(job, "target_schemas", Mock(return_value=metadata))
    load = Mock()
    monkeypatch.setattr(job, "load_batch", load)
    result = job.run(
        spark, uri, CloudberrySettings("host", 7000, "db", "user", "secret")
    )
    assert result == {
        "batch_id": "demo",
        "status": "complete",
        "tables": {"movies": 1, "movie_genres": 1, "movie_countries": 0},
    }
    loaded = load.call_args.args[1]
    assert set(loaded) == set(tables)
    assert all(not df.is_cached for df in loaded.values())
    db.close.assert_called_once()


def test_bad_manifest_never_opens_database(spark, cleaned_batch, monkeypatch):
    value, path, uri, _ = cleaned_batch
    value["status"] = "writing"
    path.write_text(json.dumps(value))
    connect = Mock()
    monkeypatch.setattr(job, "JdbcConnection", connect)
    with pytest.raises(StagingValidationError, match="not complete"):
        job.run(spark, uri, Mock())
    connect.assert_not_called()


def test_bad_parquet_never_writes_staging(spark, cleaned_batch, monkeypatch):
    value, path, uri, tables = cleaned_batch
    value["tables"]["movies"]["rows"] = 2
    value["stats"].update(read=2, movies=2)
    value["source"]["records"] = 2
    path.write_text(json.dumps(value))
    db = Mock()
    monkeypatch.setattr(job, "JdbcConnection", Mock(return_value=db))
    monkeypatch.setattr(
        job,
        "target_schemas",
        lambda _: {t: target_columns(d.schema) for t, d in tables.items()},
    )
    load = Mock()
    monkeypatch.setattr(job, "load_batch", load)
    with pytest.raises(StagingValidationError, match="row count"):
        job.run(spark, uri, Mock())
    load.assert_not_called()
    db.close.assert_called_once()


@pytest.mark.parametrize("failure", ["write", "verify", "publish"])
def test_load_rolls_back_and_cleans_private_tables(monkeypatch, failure):
    db = Mock()
    tables = dict.fromkeys(jdbc.LOAD_CONTRACT, Mock())
    write, verify, publish = Mock(), Mock(), Mock()
    {"write": write, "verify": verify, "publish": publish}[
        failure
    ].side_effect = OSError("injected")
    monkeypatch.setattr(jdbc, "write_jdbc", write)
    monkeypatch.setattr(jdbc, "verify_loaded", verify)
    monkeypatch.setattr(jdbc, "replace_batch", publish)
    with pytest.raises(OSError, match="injected"):
        jdbc.load_batch(db, tables, Mock(), "demo", dict.fromkeys(tables, 0))
    db.rollback.assert_called_once()
    drops = [
        call.args[0]
        for call in db.execute.call_args_list
        if call.args[0].startswith("DROP")
    ]
    assert len(drops) == 3
    assert all('"stg"."_load_' in sql for sql in drops)
    if failure != "publish":
        publish.assert_not_called()


def test_s3_uris_use_hadoop_s3a():
    assert job.s3a_uri("s3://bucket/key") == "s3a://bucket/key"
    assert job.s3a_uri("s3a://bucket/key") == "s3a://bucket/key"
    with pytest.raises(StagingValidationError):
        job.s3a_uri("file:///tmp/data")
