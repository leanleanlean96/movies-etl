"""Opt-in tests against a disposable database on the local Cloudberry cluster.

RUN_CLOUDBERRY_TESTS=1, CLOUDBERRY_* and the PostgreSQL JAR in Spark's
classpath are required. No API calls or changes to the etl database are made.
"""

import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import kinopoisk_stg_jdbc as jdbc
import psycopg2
import pytest
from kinopoisk_cleaned.schema import SCHEMA
from kinopoisk_cleaned.transformations import to_countries, to_genres, to_movies
from kinopoisk_stg_support import (
    StagingValidationError,
    cloudberry_settings,
    validate_tables,
)
from psycopg2 import sql
from pyspark.sql import functions as F

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_CLOUDBERRY_TESTS") != "1",
    reason="Set RUN_CLOUDBERRY_TESTS=1 to use a disposable Cloudberry database",
)


@pytest.fixture(scope="module")
def database():
    settings = cloudberry_settings()
    admin = psycopg2.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        dbname="postgres",
    )
    admin.autocommit = True
    name = "stg_test_" + uuid4().hex
    with admin.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        yield replace(settings, database=name)
    finally:
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
        admin.close()


@pytest.fixture
def db(spark, database):
    connection = jdbc.JdbcConnection(spark, database)
    ddl = Path(__file__).parents[2] / "infra/cloudberry/sql/staging.sql"
    for statement in ddl.read_text().split(";"):
        if statement.strip():
            connection.execute(statement)
    for contract in jdbc.LOAD_CONTRACT.values():
        connection.execute(f"TRUNCATE {jdbc.relation(contract['target'])}")
    connection.commit()
    try:
        yield connection, database
    finally:
        connection.close()


def batch(spark, batch_id, *, empty=False, name="Movie"):
    now = datetime(2026, 11, 1, 12, 30, tzinfo=UTC)
    raw = (
        spark.createDataFrame(
            []
            if empty
            else [
                {
                    "id": 1,
                    "name": name,
                    "year": 2020,
                    "genres": [{"id": 5, "name": "Drama", "slug": "drama"}],
                    "countries": [{"id": 10, "name": "France"}],
                }
            ],
            SCHEMA,
        )
        .withColumn("source_updated_at", F.lit(now))
        .withColumn("source_page", F.lit(1))
    )
    return {
        "movies": to_movies(spark, raw, batch_id=batch_id, ingested_at=now),
        "movie_genres": to_genres(spark, raw, batch_id),
        "movie_countries": to_countries(spark, raw, batch_id),
    }


def load(spark, db, settings, batch_id, **kwargs):
    tables = batch(spark, batch_id, **kwargs)
    counts = {table: df.count() for table, df in tables.items()}
    # Match the production checks before JDBC as well as the post-write checks.
    manifest = {
        "manifest_version": 1,
        "status": "complete",
        "batch_id": batch_id,
        "source": {"records": counts["movies"]},
        "stats": {
            "read": counts["movies"],
            "movies": counts["movies"],
            "genre_links": counts["movie_genres"],
            "country_links": counts["movie_countries"],
            "quarantined": 0,
            "duplicates": 0,
        },
        "tables": {
            table: {
                "uri": f"s3a://etl-cleaned/kinopoisk/{table}/batch_id={batch_id}",
                "rows": counts.get(table, 0),
            }
            for table in (*jdbc.LOAD_CONTRACT, "quarantine")
        },
    }
    validate_tables(tables, manifest, jdbc.target_schemas(db))
    db.commit()
    jdbc.load_batch(db, tables, settings, batch_id, counts)


def snapshot(db):
    return {
        table: db.query(
            f"SELECT * FROM {jdbc.relation(c['target'])} ORDER BY batch_id, id"
        )
        for table, c in jdbc.LOAD_CONTRACT.items()
    }


def assert_no_private_tables(db):
    assert not db.query(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'stg' AND left(table_name, 6) = '_load_'"
    )
    db.commit()


def test_replay_replaces_all_tables_and_preserves_other_batch(spark, db):
    connection, settings = db
    load(spark, connection, settings, "other", name="Unchanged")
    batch_id = "quoted'batch"
    load(spark, connection, settings, batch_id)
    before = snapshot(connection)
    connection.commit()
    load(spark, connection, settings, batch_id)
    assert snapshot(connection) == before
    connection.commit()
    load(spark, connection, settings, batch_id, empty=True)
    result = snapshot(connection)
    assert all(
        len(rows) == 1 and rows[0]["batch_id"] == "other" for rows in result.values()
    )
    assert result["movies"][0]["name"] == "Unchanged"
    assert_no_private_tables(connection)


@pytest.mark.parametrize("failure", ["write", "publish", "duplicate-task"])
def test_failure_preserves_previously_published_batch(spark, db, monkeypatch, failure):
    connection, settings = db
    load(spark, connection, settings, "retry")
    before = snapshot(connection)
    connection.commit()
    if failure in {"write", "duplicate-task"}:
        original = jdbc.write_jdbc
        calls = 0

        def fail_write(df, target, config):
            nonlocal calls
            calls += 1
            if failure == "write" and calls == 2:
                raise RuntimeError("injected write failure")
            original(df, target, config)
            if failure == "duplicate-task" and calls == 1:
                original(df, target, config)

        monkeypatch.setattr(jdbc, "write_jdbc", fail_write)
    else:
        original_execute = connection.execute

        def fail_publish(statement, params=()):
            if statement.startswith('INSERT INTO "stg"."movie_genres"'):
                # Another connection still sees the whole previous batch mid-replacement.
                observer = jdbc.JdbcConnection(spark, settings)
                try:
                    assert snapshot(observer) == before
                finally:
                    observer.close()
                raise RuntimeError("injected publish failure")
            return original_execute(statement, params)

        monkeypatch.setattr(connection, "execute", fail_publish)
    expected = StagingValidationError if failure == "duplicate-task" else RuntimeError
    with pytest.raises(expected):
        load(spark, connection, settings, "retry", name="Must roll back")
    assert snapshot(connection) == before
    assert_no_private_tables(connection)
