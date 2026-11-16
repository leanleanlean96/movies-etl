"""Explicit transactional catalog migration; default/dry-run makes no changes.

Run with the same CLOUDBERRY_* connection settings as build.py:
  python dbt/scripts/migrate_catalog.py --dry-run
  python dbt/scripts/migrate_catalog.py --apply
  python dbt/scripts/migrate_catalog.py --reverse --apply

Only the catalog OID changes. Unknown dependencies or metadata block before copy.
No implicit migration, CASCADE, history recomputation or retention is performed.
"""

import argparse
import json
import sys
from contextlib import closing
from uuid import uuid4

import psycopg2
from build import connect
from physical_policies import (
    CATALOG_TABLE,
    STAGING_LOCK,
    analyze_catalog,
    ensure_months,
    months,
    validate_catalog,
)
from psycopg2 import sql


def preflight(cursor):
    cursor.execute("SELECT version()")
    if "Cloudberry 2.1.0" not in cursor.fetchone()[0]:
        raise RuntimeError("Migration validated only for Cloudberry 2.1.0")
    cursor.execute("""SELECT c.oid, c.relkind, r.rolname, obj_description(c.oid,'pg_class'),
        c.reloptions, c.reltablespace, c.relpersistence, c.relrowsecurity,
        c.relforcerowsecurity, c.relreplident, c.relispartition,
        (c.relam=0 OR c.relam=(SELECT oid FROM pg_am WHERE amname='heap')),
        pg_has_role(current_user,c.relowner,'USAGE'),
        has_schema_privilege(current_user,'core','CREATE')
        FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner
        WHERE c.oid=to_regclass('core.fact_movie_catalog_daily')""")
    row = cursor.fetchone()
    if not row:
        raise RuntimeError("Catalog does not exist; initialize with build.py")
    (
        oid,
        kind,
        owner,
        comment,
        options,
        tablespace,
        persistence,
        rls,
        force_rls,
        identity,
        is_leaf,
        heap_storage,
        owner_rights,
        schema_rights,
    ) = row
    if (
        kind not in ("r", "p")
        or options
        or tablespace
        or persistence != "p"
        or rls
        or force_rls
        or identity != "d"
        or is_leaf
        or not heap_storage
    ):
        raise RuntimeError("Unsupported catalog storage metadata")
    if not owner_rights or not schema_rights:
        raise RuntimeError("Migration requires owner role membership and core CREATE")
    validate_catalog(cursor, partitioned=kind == "p")
    cursor.execute(
        """SELECT attname, format_type(atttypid,atttypmod), attnotnull,
        col_description(attrelid,attnum), attacl, atthasdef, attidentity, attgenerated,
        attstorage, attstattarget, attoptions, attfdwoptions, attcollation,
        (SELECT typcollation FROM pg_type WHERE oid=atttypid), attcompression
        FROM pg_attribute WHERE attrelid=%s AND attnum>0 AND NOT attisdropped
        ORDER BY attnum""",
        (oid,),
    )
    columns = cursor.fetchall()
    expected = [
        ("snapshot_date_sk", "integer"),
        ("movie_id", "bigint"),
        ("built_at", "timestamp with time zone"),
        ("batch_id", "text"),
    ]
    if [(c[0], c[1]) for c in columns] != expected:
        raise RuntimeError("Unsupported catalog columns/types")
    for col in columns:
        if (
            col[4]
            or col[5]
            or col[6]
            or col[7]
            or col[10]
            or col[11]
            or col[12] != col[13]
            or col[14]
        ):
            raise RuntimeError(f"Unsupported column metadata: {col[0]}")
    cursor.execute(
        """SELECT a.attnum FROM pg_attribute a
        WHERE a.attrelid=%s AND a.attnum>0 AND a.attisdropped""",
        (oid,),
    )
    if cursor.fetchone():
        raise RuntimeError("Dropped catalog columns require manual migration")
    # Only intrinsic row type and managed partition attachments may depend on root.
    cursor.execute(
        """SELECT pg_describe_object(d.classid,d.objid,d.objsubid)
        FROM pg_depend d WHERE d.refclassid='pg_class'::regclass AND d.refobjid=%s
        AND NOT (d.classid='pg_type'::regclass AND d.deptype='i')
        AND NOT (d.classid='pg_class'::regclass AND d.deptype='i'
            AND d.objid=(SELECT reltoastrelid FROM pg_class WHERE oid=d.refobjid))
        AND NOT (d.classid='pg_class'::regclass AND d.objid=d.refobjid
            AND d.objsubid=1 AND d.refobjsubid=0 AND d.deptype='i')
        AND NOT (d.classid='pg_class'::regclass AND d.deptype IN ('a','P','S')
            AND d.objid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent=%s))""",
        (oid, oid),
    )
    dependencies = cursor.fetchall()
    if dependencies:
        raise RuntimeError(f"Unknown catalog dependencies: {dependencies!r}")
    # Dropping the parent also removes every leaf and its row/array types.
    cursor.execute(
        """WITH relations AS (
            SELECT %s::oid AS oid UNION ALL
            SELECT inhrelid FROM pg_inherits WHERE inhparent=%s
        )
        SELECT pg_describe_object(d.classid,d.objid,d.objsubid)
        FROM pg_depend d JOIN pg_type t ON t.oid=d.refobjid
        WHERE d.refclassid='pg_type'::regclass
          AND (t.typrelid IN (SELECT oid FROM relations)
            OR t.typelem IN (SELECT reltype FROM pg_class
                            WHERE oid IN (SELECT oid FROM relations)))
          AND NOT (d.classid='pg_type'::regclass AND d.deptype='i'
              AND d.objid=t.typarray)""",
        (oid, oid),
    )
    type_dependencies = cursor.fetchall()
    if type_dependencies:
        raise RuntimeError(f"Unknown catalog type dependencies: {type_dependencies!r}")
    cursor.execute(
        """SELECT 1 FROM pg_constraint WHERE conrelid=%s OR confrelid=%s
        UNION ALL SELECT 1 FROM pg_index WHERE indrelid=%s
        UNION ALL SELECT 1 FROM pg_trigger WHERE tgrelid=%s
        UNION ALL SELECT 1 FROM pg_rewrite WHERE ev_class=%s
        UNION ALL SELECT 1 FROM pg_seclabel WHERE classoid='pg_class'::regclass
            AND objoid=%s
        UNION ALL SELECT 1 FROM pg_inherits WHERE inhrelid=%s LIMIT 1""",
        (oid,) * 7,
    )
    if cursor.fetchone():
        raise RuntimeError(
            "Unsupported catalog constraints/indexes/triggers/rules/labels/inheritance"
        )
    cursor.execute(
        """SELECT (SELECT rolname FROM pg_roles WHERE oid=grantor), grantee,
        CASE WHEN grantee=0 THEN 'PUBLIC' ELSE (SELECT rolname FROM pg_roles WHERE oid=grantee) END,
        privilege_type, is_grantable
        FROM pg_class c, LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner)))
        WHERE c.oid=%s ORDER BY 2,4""",
        (oid,),
    )
    grants = cursor.fetchall()
    if any(g[0] != owner for g in grants):
        raise RuntimeError("Non-owner ACL grantors require manual migration")
    if kind == "p":
        # Apply the parent storage guards to leaves too: local security/identity
        # flags and other customizations cannot survive a plain replacement.
        cursor.execute(
            """SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
            JOIN pg_class p ON p.oid=i.inhparent
            WHERE i.inhparent=%s AND (c.relowner<>p.relowner
                OR EXISTS ((SELECT * FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))))
                    EXCEPT (SELECT * FROM aclexplode(coalesce(p.relacl,acldefault('r',p.relowner)))))
                OR EXISTS ((SELECT * FROM aclexplode(coalesce(p.relacl,acldefault('r',p.relowner))))
                    EXCEPT (SELECT * FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner)))))
                OR c.reloptions IS NOT NULL OR c.reltablespace<>0
                OR c.relpersistence<>'p' OR c.relrowsecurity OR c.relforcerowsecurity
                OR c.relreplident<>'d'
                OR EXISTS (SELECT 1 FROM pg_seclabel s
                    WHERE s.classoid='pg_class'::regclass AND s.objoid=c.oid)
                OR c.relam<>(SELECT oid FROM pg_am WHERE amname='heap')
                OR obj_description(c.oid,'pg_class') IS NOT NULL
                OR EXISTS (SELECT 1 FROM pg_depend d WHERE d.refclassid='pg_class'::regclass
                    AND d.refobjid=c.oid AND NOT
                        (d.classid='pg_type'::regclass AND d.deptype='i')
                        AND NOT (d.classid='pg_class'::regclass AND d.deptype='i'
                            AND d.objid=c.reltoastrelid))
                OR EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid
                    AND a.attnum>0 AND (a.attacl IS NOT NULL OR a.atthasdef
                        OR a.attisdropped OR a.attidentity<>'' OR a.attgenerated<>''
                        OR col_description(a.attrelid,a.attnum) IS NOT NULL
                        OR EXISTS (SELECT 1 FROM pg_attribute pa WHERE pa.attrelid=p.oid
                            AND pa.attnum=a.attnum AND (a.attstorage<>pa.attstorage
                                OR a.attstattarget<>pa.attstattarget
                                OR a.attnotnull<>pa.attnotnull
                                OR a.attcollation<>pa.attcollation))
                        OR a.attoptions IS NOT NULL OR a.attfdwoptions IS NOT NULL
                        OR a.attcompression<>'')))""",
            (oid,),
        )
        custom = cursor.fetchall()
        if custom:
            raise RuntimeError(f"Unsupported leaf metadata/dependencies: {custom!r}")
    cursor.execute(
        """SELECT 1 FROM pg_class t JOIN pg_class c ON c.reltoastrelid=t.oid
        WHERE (c.oid=%s OR c.oid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent=%s))
          AND (t.reloptions IS NOT NULL OR t.reltablespace<>0)""",
        (oid, oid),
    )
    if cursor.fetchone():
        raise RuntimeError("Unsupported catalog TOAST settings")
    months(cursor, CATALOG_TABLE)  # Validate every existing calendar key, even dry-run.
    return {
        "oid": oid,
        "partitioned": kind == "p",
        "owner": owner,
        "comment": comment,
        "columns": columns,
        "grants": grants,
    }


def restore_metadata(cursor, table, metadata):
    cursor.execute(
        sql.SQL("COMMENT ON TABLE {} IS {}").format(
            table, sql.Literal(metadata["comment"])
        )
    )
    for col in metadata["columns"]:
        cursor.execute(
            sql.SQL("COMMENT ON COLUMN {}.{} IS {}").format(
                table, sql.Identifier(col[0]), sql.Literal(col[3])
            )
        )
        cursor.execute(
            sql.SQL("ALTER TABLE {} ALTER COLUMN {} SET STATISTICS {}").format(
                table, sql.Identifier(col[0]), sql.Literal(col[9])
            )
        )
        # LIKE preserves NOT NULL; preserve explicit storage settings as well.
        storage = {"p": "PLAIN", "e": "EXTERNAL", "m": "MAIN", "x": "EXTENDED"}[col[8]]
        cursor.execute(
            sql.SQL("ALTER TABLE {} ALTER COLUMN {} SET STORAGE {}").format(
                table, sql.Identifier(col[0]), sql.SQL(storage)
            )
        )
    # Clear default ACLs before reproducing the original grants exactly.
    cursor.execute(
        """SELECT DISTINCT grantee FROM pg_class c,
        LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner)))
        WHERE c.oid=to_regclass(%s)""",
        (table.as_string(cursor),),
    )
    for (grantee,) in cursor.fetchall():
        if grantee == 0:
            role = sql.SQL("PUBLIC")
        else:
            cursor.execute("SELECT rolname FROM pg_roles WHERE oid=%s", (grantee,))
            role = sql.Identifier(cursor.fetchone()[0])
        cursor.execute(sql.SQL("REVOKE ALL ON {} FROM {}").format(table, role))
    cursor.execute(
        sql.SQL("ALTER TABLE {} OWNER TO {}").format(
            table, sql.Identifier(metadata["owner"])
        )
    )
    cursor.execute(
        sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(metadata["owner"]))
    )
    for _, grantee, name, privilege, grantable in metadata["grants"]:
        cursor.execute(
            sql.SQL("GRANT {} ON {} TO {} {}").format(
                sql.SQL(privilege),
                table,
                sql.SQL("PUBLIC") if grantee == 0 else sql.Identifier(name),
                sql.SQL("WITH GRANT OPTION" if grantable else ""),
            )
        )
    cursor.execute("RESET ROLE")


def migrate(connection, *, reverse=False, apply=False, fail_after=None):
    """Return preflight/result. fail_after='copy'/'swap' is for rollback verification."""
    with connection, connection.cursor() as cursor:
        cursor.execute("SET LOCAL TIME ZONE 'UTC'")
        cursor.execute("SET LOCAL statement_timeout='15min'")
        cursor.execute("SET LOCAL lock_timeout='30s'")
        cursor.execute(STAGING_LOCK)
        cursor.execute(
            sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(CATALOG_TABLE)
        )
        metadata = preflight(cursor)
        result = {
            "old_oid": metadata["oid"],
            "direction": "plain" if reverse else "partitioned",
            "apply": apply,
            "changed": False,
        }
        if metadata["partitioned"] == (not reverse):
            result["status"] = "already configured"
            return result
        if not apply:
            result["status"] = "preflight passed; no changes"
            return result
        prefix = "_catalog_migrate_" + uuid4().hex[:12]
        replacement = sql.Identifier("core", prefix)
        cursor.execute(
            sql.SQL(
                "CREATE TABLE {} (LIKE {} INCLUDING ALL) DISTRIBUTED BY (movie_id) {}"
            ).format(
                replacement,
                CATALOG_TABLE,
                sql.SQL("" if reverse else "PARTITION BY RANGE(snapshot_date_sk)"),
            )
        )
        if not reverse:
            ensure_months(cursor, CATALOG_TABLE, replacement, prefix)
        cursor.execute(
            sql.SQL("INSERT INTO {} SELECT * FROM {}").format(
                replacement, CATALOG_TABLE
            )
        )
        cursor.execute(
            sql.SQL(
                "SELECT (SELECT count(*) FROM {}), (SELECT count(*) FROM {}), "
                "EXISTS ((SELECT * FROM {} EXCEPT ALL SELECT * FROM {}) "
                "UNION ALL (SELECT * FROM {} EXCEPT ALL SELECT * FROM {}))"
            ).format(
                CATALOG_TABLE,
                replacement,
                CATALOG_TABLE,
                replacement,
                replacement,
                CATALOG_TABLE,
            )
        )
        before, after, difference = cursor.fetchone()
        if before != after or difference:
            raise RuntimeError("Migration row count/EXCEPT ALL mismatch")
        if fail_after == "copy":
            raise RuntimeError("Injected migration failure after copy")
        restore_metadata(cursor, replacement, metadata)
        # No external dependencies are accepted: DROP RESTRICT is an additional guard.
        cursor.execute(sql.SQL("DROP TABLE {} RESTRICT").format(CATALOG_TABLE))
        cursor.execute(
            sql.SQL("ALTER TABLE {} RENAME TO fact_movie_catalog_daily").format(
                replacement
            )
        )
        validate_catalog(cursor, partitioned=not reverse)
        if fail_after == "swap":
            raise RuntimeError("Injected migration failure after swap")
        analyze_catalog(cursor)
        cursor.execute("SELECT 'core.fact_movie_catalog_daily'::regclass::oid")
        result.update(
            new_oid=cursor.fetchone()[0], rows=after, changed=True, status="migrated"
        )
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Preflight only (default)")
    mode.add_argument(
        "--apply", action="store_true", help="Commit the explicit migration"
    )
    parser.add_argument(
        "--reverse", action="store_true", help="Copy back to plain storage"
    )
    args = parser.parse_args()
    with closing(connect()) as connection:
        print(json.dumps(migrate(connection, reverse=args.reverse, apply=args.apply)))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, psycopg2.Error) as error:
        print(f"Migration rolled back: {error}", file=sys.stderr)
        sys.exit(1)
