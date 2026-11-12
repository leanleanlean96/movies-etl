"""RUN_PUBLISH_TESTS=1: real PostgreSQL engine and refreshable MV, disposable DBs."""

import os
import re
from contextlib import closing
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import clickhouse_connect
import psycopg2
import pytest
from psycopg2 import sql

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_PUBLISH_TESTS") != "1",
    reason="Set RUN_PUBLISH_TESTS=1 for real database tests",
)
SQL_ROOT = Path(__file__).parents[2] / "infra/clickhouse/sql"
CONTRACTS = re.findall(
    r"CREATE TABLE IF NOT EXISTS ext_cloudberry\.(\w+) \(\n(.*?)\n\) ENGINE",
    (SQL_ROOT / "01_sources.sql").read_text(),
    re.DOTALL,
)


def fields(definition):
    return re.findall(r"`(\w+)` (.+?)(?:,\n|$)", definition)


def postgres_type(kind):
    kind = kind.removeprefix("Nullable(")
    if kind.startswith("Decimal("):
        # Exercise the actual source's numeric columns without typmods as well.
        return "numeric"
    if kind.startswith("DateTime64"):
        return "timestamptz"
    return {
        "Int64": "bigint",
        "Int32": "integer",
        "Int16": "smallint",
        "Date32": "date",
        "String": "text",
        "Bool": "boolean",
    }[kind.rstrip(")")]


def sample_value(name, kind):
    if name == "production_year":
        return None
    if "Decimal(30" in kind:
        return (
            Decimal("12345678901234567890123.123456")
            if ", 6" in kind
            else Decimal("12345678901234567890123.12")
        )
    if "Decimal(20" in kind:
        return Decimal("123456789012345678.12")
    if "Decimal" in kind:
        return Decimal("1.25")
    if "DateTime64" in kind:
        return datetime(2026, 11, 12, 10, 0, 0, 123456, tzinfo=UTC)
    if "Date32" in kind:
        return date(2026, 11, 9)
    if "String" in kind:
        return "Тест ' \\"
    if "Bool" in kind:
        return True
    return 1


@pytest.fixture
def warehouse():
    name = "refresh_test_" + uuid4().hex
    external, collection = name + "_ext", name + "_connection"
    settings = {
        "host": os.getenv("CLOUDBERRY_HOST", "127.0.0.1"),
        "port": int(os.getenv("CLOUDBERRY_PORT", "7000")),
        "user": os.getenv("CLOUDBERRY_USER", "gpadmin"),
        "password": os.environ["CLOUDBERRY_PASSWORD"],
    }
    with (
        closing(psycopg2.connect(**settings, dbname="postgres")) as admin,
        closing(
            clickhouse_connect.get_client(
                host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
                port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
                username=os.getenv("CLICKHOUSE_USER", "etl"),
                password=os.environ["CLICKHOUSE_PASSWORD"],
                database="default",
                query_retries=0,
            )
        ) as client,
    ):
        admin.autocommit = True
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            with closing(psycopg2.connect(**settings, dbname=name)) as connection:
                with connection, connection.cursor() as cursor:
                    cursor.execute("CREATE SCHEMA mart")
                    for table, definition in CONTRACTS:
                        columns = fields(definition)
                        cursor.execute(
                            sql.SQL("CREATE TABLE {} ({})").format(
                                sql.Identifier("mart", table),
                                sql.SQL(", ").join(
                                    sql.SQL("{} {}").format(
                                        sql.Identifier(column),
                                        sql.SQL(postgres_type(kind)),
                                    )
                                    for column, kind in columns
                                ),
                            )
                        )
                        values = [
                            sample_value(column, kind) for column, kind in columns
                        ]
                        cursor.execute(
                            sql.SQL("INSERT INTO {} VALUES ({})").format(
                                sql.Identifier("mart", table),
                                sql.SQL(", ").join(sql.Placeholder() for _ in values),
                            ),
                            values,
                        )

                client.command(
                    f"CREATE NAMED COLLECTION {collection} AS "
                    "host=%(host)s, port=%(port)s, database=%(database)s, "
                    "user=%(user)s, password=%(password)s, schema='mart'",
                    parameters={
                        "host": "cloudberry",
                        "port": 7000,
                        "database": name,
                        "user": settings["user"],
                        "password": settings["password"],
                    },
                )

                def execute_file(filename):
                    script = "\n".join(
                        line
                        for line in (SQL_ROOT / filename).read_text().splitlines()
                        if not line.lstrip().startswith("--")
                    )
                    script = script.replace("ext_cloudberry", external)
                    script = script.replace("mart.", name + ".")
                    script = script.replace(
                        "EXISTS mart ENGINE", f"EXISTS {name} ENGINE"
                    )
                    script = script.replace("cloudberry_marts", collection)
                    for statement in script.split(";"):
                        if statement.strip():
                            client.command(statement)

                execute_file("01_sources.sql")
                execute_file("02_views.sql")
                yield connection, client, name, execute_file
        finally:
            # Drop views before removing their source connection/database.
            client.command(f"DROP DATABASE IF EXISTS {name} SYNC")
            client.command(f"DROP DATABASE IF EXISTS {external} SYNC")
            client.command(f"DROP NAMED COLLECTION IF EXISTS {collection}")
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


def snapshots(client, database):
    return {
        table: client.query(f"SELECT * FROM {database}.{table}").result_rows
        for table, _ in CONTRACTS
    }


def test_repeat_exact_types_updates_deletions_and_empty_snapshot(warehouse):
    connection, client, database, execute = warehouse
    assert all(not rows for rows in snapshots(client, database).values())
    execute("refresh.sql")
    first = snapshots(client, database)
    with connection, connection.cursor() as cursor:
        for table, _ in CONTRACTS:
            cursor.execute(
                sql.SQL("SELECT * FROM {}").format(sql.Identifier("mart", table))
            )
            # clickhouse-connect may return UTC timestamps without tzinfo.
            actual = [
                tuple(
                    value.replace(tzinfo=UTC)
                    if isinstance(value, datetime) and value.tzinfo is None
                    else value
                    for value in row
                )
                for row in first[table]
            ]
            assert cursor.fetchall() == actual
    execute("refresh.sql")
    assert snapshots(client, database) == first
    with connection, connection.cursor() as cursor:
        cursor.execute("DELETE FROM mart.mart_catalog_year_genre")
        cursor.execute(
            "UPDATE mart.mart_movie_scores_current SET rating_kp=NULL, name='', votes_kp=7"
        )
        cursor.execute("UPDATE mart.mart_top250_daily_changes SET entered=false")
    execute("refresh.sql")
    assert (
        client.command(f"SELECT count() FROM {database}.mart_catalog_year_genre") == 0
    )
    assert client.query(
        f"SELECT rating_kp,name,votes_kp FROM {database}.mart_movie_scores_current"
    ).result_rows == [(None, "", 7)]
    assert client.query(
        f"SELECT entered FROM {database}.mart_top250_daily_changes"
    ).result_rows == [(False,)]
    assert (
        client.query(
            "SELECT status FROM system.view_refreshes WHERE database={db:String}",
            parameters={"db": database},
        ).result_rows
        == [("Disabled",)] * 8
    )


def test_failed_refresh_preserves_previous_table_and_retry_recovers(warehouse):
    connection, client, database, execute = warehouse
    execute("refresh.sql")
    before = snapshots(client, database)
    with connection, connection.cursor() as cursor:
        cursor.execute("ALTER TABLE mart.mart_catalog_year_genre RENAME TO unavailable")
    try:
        with pytest.raises(Exception, match="does not exist|UNKNOWN_TABLE|relation"):
            execute("refresh.sql")
    finally:
        execute("stop.sql")
        with connection, connection.cursor() as cursor:
            cursor.execute(
                "ALTER TABLE mart.unavailable RENAME TO mart_catalog_year_genre"
            )
    assert snapshots(client, database) == before
    execute("refresh.sql")
    assert snapshots(client, database) == before
