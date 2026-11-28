"""Prepare the existing Greenplum and ClickHouse contracts for the first ETL run."""

import base64
import hashlib
import http.client
import os
import secrets
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import psycopg2

WORKSPACE = Path("/workspace")
STAGING_TABLES = {"movies", "movie_genres", "movie_countries", "top250"}
MART_TABLES = {
    "mart_box_office_current_by_market_currency",
    "mart_budget_current_by_currency",
    "mart_budget_vs_box_office",
    "mart_catalog_year_country",
    "mart_catalog_year_genre",
    "mart_movie_popularity_weekly",
    "mart_movie_scores_current",
    "mart_top250_daily_changes",
}
READER_TABLES = ("mart.mart_movie_scores_current", "mart.mart_movie_popularity_weekly")
READER_USER = "superset_reader"


def greenplum_relations(cursor, schema):
    cursor.execute(
        """SELECT c.relname
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = %s AND c.relkind IN ('r', 'p', 'v', 'm')""",
        (schema,),
    )
    return {name for (name,) in cursor}


def prepare_greenplum():
    with (
        closing(
            psycopg2.connect(
                host=os.environ["GREENPLUM_HOST"],
                port=os.environ["GREENPLUM_PORT"],
                user=os.environ["GREENPLUM_USER"],
                password=os.environ["GREENPLUM_PASSWORD"],
                dbname=os.environ.get("GREENPLUM_DATABASE", "etl"),
                connect_timeout=10,
            )
        ) as connection,
        connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("SET LOCAL lock_timeout = '30s'")
        cursor.execute(
            """SELECT count(*) FROM information_schema.columns
                WHERE table_schema = 'stg' AND table_name = 'movies'
                  AND column_name IN ('genres_complete', 'countries_complete')"""
        )
        completeness_columns = cursor.fetchone()[0]
        if (
            not STAGING_TABLES <= greenplum_relations(cursor, "stg")
            or completeness_columns != 2
        ):
            for ddl in ("staging.sql", "dbt_inputs.sql"):
                cursor.execute((WORKSPACE / "greenplum" / ddl).read_text())
            print("Greenplum staging is ready.")

        core = greenplum_relations(cursor, "core")
        marts = greenplum_relations(cursor, "mart")

    if core and MART_TABLES <= marts:
        print("Using existing Greenplum core and mart relations.")
        return
    if core or marts:
        raise RuntimeError(
            "Greenplum has an incomplete dbt deployment. Restore the missing relations "
            "before starting the stack."
        )

    print("Preparing Greenplum core and mart with the existing dbt build.")
    result = subprocess.run(
        [sys.executable, "scripts/build.py"],
        cwd=WORKSPACE / "dbt",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=1800,
        check=False,
    )
    print(result.stdout.replace(os.environ["GREENPLUM_PASSWORD"], "[redacted]"), end="")
    if result.returncode:
        raise RuntimeError(
            f"Initial dbt build failed with exit code {result.returncode}."
        )


def clickhouse_query(statement, user, password, stage, *, paused_views=False):
    encoded = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
    path = "/?wait_end_of_query=1"
    if paused_views:
        path += "&stop_refreshable_materialized_views_on_startup=1"
    with closing(
        http.client.HTTPConnection("clickhouse", 8123, timeout=45)
    ) as connection:
        connection.request(
            "POST", path, statement.encode(), {"Authorization": f"Basic {encoded}"}
        )
        response = connection.getresponse()
        if response.status != 200:
            code = response.getheader("X-ClickHouse-Exception-Code", "unknown")
            raise RuntimeError(
                f"{stage} failed (ClickHouse HTTP {response.status}, code {code})."
            )
        return response.read().decode().strip()


def create_statements(filename):
    ddl = (WORKSPACE / "clickhouse" / filename).read_text()
    ddl = "\n".join(
        line for line in ddl.splitlines() if not line.lstrip().startswith("--")
    )
    # The persisted startup setting and EMPTY create paused views. Reusing the CREATE
    # definitions without the manual STOP commands leaves existing refreshes alone.
    return [
        statement
        for part in ddl.split(";")
        if (statement := part.strip()).startswith("CREATE ")
    ]


def prepare_clickhouse(reader_password):
    def admin_query(statement, stage, *, paused_views=False):
        return clickhouse_query(
            statement,
            os.environ["CLICKHOUSE_USER"],
            os.environ["CLICKHOUSE_PASSWORD"],
            stage,
            paused_views=paused_views,
        )

    relations = set(
        admin_query(
            "SELECT concat(database, '.', name) FROM system.tables "
            "WHERE database IN ('ext_cloudberry', 'mart') FORMAT TSVRaw",
            "Read warehouse relations",
        ).splitlines()
    )
    sources = {f"ext_cloudberry.{table}" for table in MART_TABLES}
    serving = {f"mart.{table}" for table in MART_TABLES} | {
        f"mart.refresh_{table}" for table in MART_TABLES
    }
    if not sources <= relations:
        for statement in create_statements("01_sources.sql"):
            admin_query(statement, "Create ClickHouse sources")
    if not serving <= relations:
        for statement in create_statements("02_views.sql"):
            admin_query(
                statement, "Create ClickHouse serving relations", paused_views=True
            )

    salt = secrets.token_hex(32)
    digest = hashlib.sha256((reader_password + salt).encode()).hexdigest()
    admin_query(
        f"CREATE USER IF NOT EXISTS {READER_USER} IDENTIFIED WITH sha256_hash "
        f"BY '{digest}' SALT '{salt}'",
        "Create the Superset reader",
    )
    for table in READER_TABLES:
        admin_query(
            f"GRANT SELECT ON {table} TO {READER_USER}",
            f"Grant Superset access to {table}",
        )
        clickhouse_query(
            f"SELECT * FROM {table} LIMIT 0",
            READER_USER,
            reader_password,
            f"Verify {READER_USER} access to {table}",
        )
    print("ClickHouse serving relations and Superset reader are ready.")


def main():
    for name in (
        "GREENPLUM_PASSWORD",
        "CLICKHOUSE_PASSWORD",
        "CLICKHOUSE_READER_PASSWORD",
    ):
        if not os.environ.get(name):
            raise RuntimeError(f"Set {name} before starting the stack.")
    if os.environ["CLICKHOUSE_USER"] == READER_USER:
        raise RuntimeError(
            "CLICKHOUSE_USER must be the ClickHouse administrator account."
        )
    prepare_greenplum()
    prepare_clickhouse(os.environ["CLICKHOUSE_READER_PASSWORD"])


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        sys.exit(str(error))
    except (
        psycopg2.Error,
        OSError,
        http.client.HTTPException,
        subprocess.TimeoutExpired,
    ) as error:
        sys.exit(f"Warehouse initialization failed ({type(error).__name__}).")
