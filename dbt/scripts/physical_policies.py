"""Physical storage policies, independent of dbt's logical replacement keys.

Workload measurements did not justify additional indexes.
An empty index policy is intentional.
"""

import re
from datetime import date

from psycopg2 import sql

CATALOG = ("core", "fact_movie_catalog_daily")
CATALOG_TABLE = sql.Identifier(*CATALOG)
CATALOG_COLUMNS = [
    ("snapshot_date_sk", "integer"),
    ("movie_id", "bigint"),
    ("built_at", "timestamp with time zone"),
    ("batch_id", "text"),
]
CONFIRMED_INDEXES = ()
STAGING_LOCK = (
    "LOCK TABLE stg.movies, stg.movie_genres, stg.movie_countries, stg.top250 "
    "IN SHARE ROW EXCLUSIVE MODE"
)


def is_catalog(node):
    return (node["schema"], node["alias"]) == CATALOG


def partition_clause(node):
    return sql.SQL("PARTITION BY RANGE (snapshot_date_sk)" if is_catalog(node) else "")


def month_bounds(key):
    if not isinstance(key, int) or not 10101 <= key <= 99991231:
        raise RuntimeError(f"Invalid catalog calendar key: {key!r}")
    try:
        day = date(key // 10000, key // 100 % 100, key % 100)
        following = date(day.year + (day.month == 12), day.month % 12 + 1, 1)
    except ValueError as error:
        raise RuntimeError(f"Invalid catalog calendar key: {key!r}") from error
    return (
        day.year * 10000 + day.month * 100 + 1,
        following.year * 10000 + following.month * 100 + 1,
    )


def months(cursor, source):
    cursor.execute(sql.SQL("SELECT DISTINCT snapshot_date_sk FROM {}").format(source))
    return sorted({month_bounds(row[0]) for row in cursor.fetchall()})


def validate_catalog(cursor, table=CATALOG_TABLE, partitioned=True):
    """Reject incompatible parents, leaves and distribution; never migrate in build."""
    cursor.execute(
        "SELECT c.relkind, pg_get_partkeydef(c.oid) FROM pg_class c "
        "WHERE c.oid=to_regclass(%s)",
        (table.as_string(cursor),),
    )
    expected = ("p", "RANGE (snapshot_date_sk)") if partitioned else ("r", None)
    if cursor.fetchone() != expected:
        raise RuntimeError(
            "Catalog physical schema incompatible; run scripts/migrate_catalog.py "
            "--dry-run, then explicitly --apply (or --reverse for plain storage)"
        )
    cursor.execute(
        """SELECT attname, format_type(atttypid,atttypmod)
        FROM pg_attribute WHERE attrelid=to_regclass(%s) AND attnum>0
            AND NOT attisdropped ORDER BY attnum""",
        (table.as_string(cursor),),
    )
    if cursor.fetchall() != CATALOG_COLUMNS:
        raise RuntimeError("Catalog columns/types require explicit migration")
    cursor.execute(
        """SELECT p.policytype, ARRAY(SELECT a.attname::text
        FROM unnest(p.distkey) WITH ORDINALITY k(num,ord)
        JOIN pg_attribute a ON a.attrelid=p.localoid AND a.attnum=k.num ORDER BY k.ord)
        FROM gp_distribution_policy p WHERE p.localoid=to_regclass(%s)""",
        (table.as_string(cursor),),
    )
    if cursor.fetchone() != ("p", ["movie_id"]):
        raise RuntimeError("Catalog requires DISTRIBUTED BY (movie_id)")
    if not partitioned:
        return
    cursor.execute(
        """SELECT c.oid, c.relname, c.relkind, pg_get_expr(c.relpartbound,c.oid),
        p.policytype, p.distkey::text FROM pg_inherits i
        JOIN pg_class c ON c.oid=i.inhrelid
        LEFT JOIN gp_distribution_policy p ON p.localoid=c.oid
        WHERE i.inhparent=to_regclass(%s)""",
        (table.as_string(cursor),),
    )
    for oid, name, kind, bound, policy, keys in cursor.fetchall():
        # Managed leaves have exactly one calendar month and inherited distribution.
        match = re.fullmatch(r"FOR VALUES FROM \((\d+)\) TO \((\d+)\)", bound or "")
        if (
            not match
            or kind != "r"
            or policy != "p"
            or keys != "2"
            or month_bounds(int(match[1])) != (int(match[1]), int(match[2]))
        ):
            raise RuntimeError(f"Unsupported catalog partition: {name} ({oid})")


def ensure_months(cursor, source, table=CATALOG_TABLE, name_prefix=CATALOG[1]):
    required = months(cursor, source)
    cursor.execute(
        "SELECT pg_get_expr(c.relpartbound,c.oid) FROM pg_inherits i "
        "JOIN pg_class c ON c.oid=i.inhrelid WHERE i.inhparent=to_regclass(%s)",
        (table.as_string(cursor),),
    )
    existing = {row[0] for row in cursor.fetchall()}
    for start, end in required:
        bound = f"FOR VALUES FROM ({start}) TO ({end})"
        if bound not in existing:
            leaf = sql.Identifier("core", f"{name_prefix}_m{start // 100:06d}")
            cursor.execute(
                sql.SQL(
                    "CREATE TABLE {} PARTITION OF {} FOR VALUES FROM ({}) TO ({})"
                ).format(leaf, table, sql.Literal(start), sql.Literal(end))
            )
    return required


def delete_catalog_days(cursor, target, match):
    deleted = 0
    for start, end in months(cursor, sql.Identifier("_etl_changed")):
        cursor.execute(
            sql.SQL(
                "DELETE FROM {} t USING _etl_changed d WHERE "
                "t.snapshot_date_sk >= {} AND t.snapshot_date_sk < {} AND {}"
            ).format(target, sql.Literal(start), sql.Literal(end), match)
        )
        deleted += cursor.rowcount
    return deleted


def analyze_catalog(cursor, target=CATALOG_TABLE):
    # Cloudberry ANALYZE on the parent refreshes leaf and root statistics.
    cursor.execute(sql.SQL("ANALYZE {}").format(target))
