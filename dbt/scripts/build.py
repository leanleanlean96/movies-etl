"""Compile dbt SQL and publish all models/tests in one Cloudberry transaction.

No persistent technical tables. Temporary candidates disappear at commit/rollback.
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import psycopg2
from psycopg2 import sql

PROJECT = Path(__file__).resolve().parents[1]
ROOT = PROJECT.parent


def connect():
    return psycopg2.connect(
        host=os.environ.get("CLOUDBERRY_HOST", "127.0.0.1"),
        port=int(os.environ.get("CLOUDBERRY_PORT", "7000")),
        user=os.environ.get("CLOUDBERRY_USER", "gpadmin"),
        password=os.environ["CLOUDBERRY_PASSWORD"],
        dbname=os.environ.get("CLOUDBERRY_DATABASE", "etl"),
    )


def relation(node):
    return sql.Identifier(node["schema"], node["alias"])


def order_models(manifest):
    nodes = {
        k: n for k, n in manifest["nodes"].items() if n["resource_type"] == "model"
    }
    ordered = []
    visited = set()

    def visit(key):
        if key in visited:
            return
        visited.add(key)
        for parent in nodes[key]["depends_on"]["nodes"]:
            if parent in nodes:
                visit(parent)
        ordered.append(nodes[key])

    for key in sorted(nodes):
        visit(key)
    return ordered


def reconcile(cursor, node, query, full_refresh=False):
    """Replace only changed complete partitions, including disappeared partitions."""
    target = relation(node)
    cursor.execute(
        sql.SQL("CREATE TEMP TABLE _etl_incoming ON COMMIT DROP AS {}").format(
            sql.SQL(query)
        )
    )
    cursor.execute("SELECT * FROM _etl_incoming LIMIT 0")
    columns = [d.name for d in cursor.description]
    cols = sql.SQL(", ").join(map(sql.Identifier, columns))
    cursor.execute(
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM _etl_incoming WHERE false"
        ).format(target)
    )
    cursor.execute(sql.SQL("SELECT * FROM {} LIMIT 0").format(target))
    if [d.name for d in cursor.description] != columns:
        raise RuntimeError(f"Schema changed for {node['name']}; migrate explicitly")
    if node["name"] == "fact_top250_daily":
        cursor.execute(
            sql.SQL("""SELECT DISTINCT old.snapshot_date_sk FROM {} old
            WHERE NOT EXISTS (SELECT 1 FROM _etl_incoming n
                WHERE n.snapshot_date_sk = old.snapshot_date_sk)
              AND EXISTS (SELECT 1 FROM stg.top250 s
                WHERE to_char(s.snapshot_date, 'YYYYMMDD')::integer = old.snapshot_date_sk)
            LIMIT 1""").format(target)
        )
        if cursor.fetchone():
            raise RuntimeError("Incomplete replacement of a published top250 snapshot")
    if full_refresh:
        cursor.execute(sql.SQL("DELETE FROM {}").format(target))
        deleted = cursor.rowcount
        cursor.execute(
            sql.SQL("INSERT INTO {} SELECT * FROM _etl_incoming").format(target)
        )
    else:
        keys = node["config"].get("partition_keys") or node["config"]["unique_key"]
        keycols = sql.SQL(", ").join(map(sql.Identifier, keys))
        cursor.execute(
            sql.SQL("""CREATE TEMP TABLE _etl_changed ON COMMIT DROP AS
            SELECT DISTINCT {} FROM (
                (SELECT {} FROM {} EXCEPT SELECT {} FROM _etl_incoming)
                UNION ALL
                (SELECT {} FROM _etl_incoming EXCEPT SELECT {} FROM {})
            ) delta""").format(keycols, cols, target, cols, cols, cols, target)
        )
        match = sql.SQL(" AND ").join(
            sql.SQL("t.{} IS NOT DISTINCT FROM d.{}").format(
                sql.Identifier(k), sql.Identifier(k)
            )
            for k in keys
        )
        cursor.execute(
            sql.SQL("DELETE FROM {} t USING _etl_changed d WHERE {}").format(
                target, match
            )
        )
        deleted = cursor.rowcount
        cursor.execute(
            sql.SQL("""INSERT INTO {} SELECT t.* FROM _etl_incoming t
            WHERE EXISTS (SELECT 1 FROM _etl_changed d WHERE {})""").format(
                target, match
            )
        )
    inserted = cursor.rowcount
    if not full_refresh:
        cursor.execute("DROP TABLE _etl_changed")
    cursor.execute("DROP TABLE _etl_incoming")
    print(
        f"{node['schema']}.{node['name']}: deleted={deleted}, inserted={inserted}",
        flush=True,
    )


def seed(cursor, name):
    with (PROJECT / "seeds" / f"{name}.csv").open() as f:
        rows = list(csv.reader(f))
    columns = rows.pop(0)
    types = ["integer"] + ["text"] * (len(columns) - 1)
    definition = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t))
        for c, t in zip(columns, types)
    )
    table = sql.Identifier("core", name)
    cursor.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(table, definition)
    )
    cursor.execute(sql.SQL("DELETE FROM {}").format(table))
    for row in rows:
        cursor.execute(
            sql.SQL("INSERT INTO {} VALUES ({})").format(
                table, sql.SQL(", ").join([sql.Placeholder()] * len(row))
            ),
            [v or None for v in row],
        )


def compiled_query(node):
    query = node.get("compiled_code")
    if not query:
        raise RuntimeError(f"Missing compiled SQL: {node['name']}")
    return query.strip().rstrip(";")


def check(cursor, node):
    cursor.execute(
        sql.SQL("SELECT * FROM ({}) failures LIMIT 5").format(
            sql.SQL(compiled_query(node))
        )
    )
    rows = cursor.fetchall()
    if rows:
        raise RuntimeError(f"Test {node['name']} failed; sample={rows!r}")


def publish(connection, manifest, full_refresh=False, fail_after=None):
    with connection, connection.cursor() as cursor:
        cursor.execute("SET LOCAL TIME ZONE 'UTC'")
        cursor.execute("SET LOCAL lock_timeout = '30s'")
        cursor.execute("SET LOCAL statement_timeout = '15min'")
        # Serialize publishers and prevent staging replacement during the build.
        cursor.execute(
            "LOCK TABLE stg.movies, stg.movie_genres, stg.movie_countries, stg.top250 IN SHARE ROW EXCLUSIVE MODE"
        )
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS mart")
        for name in ["currency_mapping", "market_mapping"]:
            seed(cursor, name)
        tests = [n for n in manifest["nodes"].values() if n["resource_type"] == "test"]
        for node in tests:
            if "input_check" in node["tags"]:
                check(cursor, node)
        for node in order_models(manifest):
            query = compiled_query(node)
            if node["config"]["materialized"] == "view":
                cursor.execute(
                    sql.SQL("CREATE OR REPLACE VIEW {} AS {}").format(
                        relation(node), sql.SQL(query)
                    )
                )
            else:
                reconcile(cursor, node, query, full_refresh)
            if node["name"] == fail_after:
                raise RuntimeError("Injected publish failure")
        for node in tests:
            if "input_check" not in node["tags"]:
                check(cursor, node)
        print(
            f"All {len(tests)} data tests passed; committing core and mart together.",
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument(
        "--snapshot-end-date", help="UTC date; default is the dbt invocation date"
    )
    parser.add_argument(
        "--skip-compile",
        action="store_true",
        help="Reuse a previously compiled manifest with identical vars",
    )
    args = parser.parse_args()
    target = Path(os.environ.get("DBT_TARGET_PATH", str(PROJECT / "target")))
    if not target.is_absolute():
        target = PROJECT / target
    os.environ.setdefault("DBT_LOG_PATH", str(target / "logs"))
    if not args.skip_compile:
        command = ["dbt", "compile", "--project-dir", str(PROJECT)]
        if args.snapshot_end_date:
            command += [
                "--vars",
                json.dumps({"snapshot_end_date": args.snapshot_end_date}),
            ]
        subprocess.run(command, check=True)
    manifest = json.loads((target / "manifest.json").read_text())
    with closing(connect()) as connection:
        publish(connection, manifest, args.full_refresh)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, psycopg2.Error, subprocess.CalledProcessError) as error:
        print(f"Build rolled back: {error}", file=sys.stderr)
        sys.exit(1)
