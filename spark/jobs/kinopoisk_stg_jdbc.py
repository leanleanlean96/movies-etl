"""JDBC writes are isolated from stg until all three tables can be committed."""

import logging
from contextlib import contextmanager
from uuid import uuid4

from kinopoisk_stg_support import (
    LOAD_CONTRACT,
    StagingValidationError,
    validate_target_schema,
)

logger = logging.getLogger(__name__)


def identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def relation(value: str) -> str:
    return ".".join(identifier(part) for part in value.split("."))


class JdbcConnection:
    """Small driver-side SQL adapter; row data is written by Spark's JDBC writer."""

    def __init__(self, spark, settings):
        props = spark._jvm.java.util.Properties()
        for key, value in jdbc_properties(settings).items():
            props.setProperty(key, value)
        self.connection = spark._jvm.org.postgresql.Driver().connect(
            settings.jdbc_url, props
        )
        self.connection.setAutoCommit(False)

    @contextmanager
    def statement(self, sql, params=()):
        statement = self.connection.prepareStatement(sql)
        try:
            for index, value in enumerate(params, start=1):
                statement.setString(index, value)
            yield statement
        finally:
            statement.close()

    def execute(self, sql, params=()):
        with self.statement(sql, params) as statement:
            statement.execute()

    def query(self, sql, params=()):
        with self.statement(sql, params) as statement:
            result = statement.executeQuery()
            try:
                metadata = result.getMetaData()
                names = [
                    metadata.getColumnLabel(index)
                    for index in range(1, metadata.getColumnCount() + 1)
                ]
                rows = []
                while result.next():
                    rows.append(
                        {
                            name: result.getObject(index)
                            for index, name in enumerate(names, start=1)
                        }
                    )
                return rows
            finally:
                result.close()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()


def jdbc_properties(settings):
    return {
        "user": settings.user,
        "password": settings.password,
        "driver": "org.postgresql.Driver",
        "options": "-c timezone=UTC",
        "connectTimeout": "15",
        "socketTimeout": "600",
        "ApplicationName": "kinopoisk_cleaned_to_stg",
    }


def target_schemas(db):
    schemas = {}
    for table, contract in LOAD_CONTRACT.items():
        schema, name = contract["target"].split(".")
        schemas[table] = db.query(
            "SELECT column_name, data_type, is_nullable, column_default, "
            "numeric_precision, numeric_scale, character_maximum_length, "
            "is_generated, is_identity, identity_generation "
            "FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            (schema, name),
        )
        if not schemas[table]:
            raise StagingValidationError(
                f"Missing target {contract['target']}; apply infra/cloudberry/sql/staging.sql"
            )
    return schemas


def write_jdbc(df, target, settings):
    # No automatic table creation: these tables retain Cloudberry types and distribution.
    df.coalesce(1).write.jdbc(
        settings.jdbc_url,
        relation(target),
        mode="append",
        properties={
            **jdbc_properties(settings),
            "batchsize": "1000",
            "numPartitions": "1",
            "isolationLevel": "READ_COMMITTED",
        },
    )


def verify_loaded(db, staging, batch_id, counts):
    """Also detect duplicate inserts caused by a retried Spark JDBC task."""
    for table, contract in LOAD_CONTRACT.items():
        target = relation(staging[table])
        rows = db.query(f"SELECT count(*) AS rows FROM {target}")[0]["rows"]
        if rows != counts[table]:
            raise StagingValidationError(
                f"{table}: loaded row count differs from manifest"
            )
        required = set(contract["key"]) | set(contract["required"])
        invalid = " OR ".join(
            f"{identifier(column)} IS NULL" for column in sorted(required)
        )
        if db.query(
            f"SELECT 1 FROM {target} WHERE batch_id IS DISTINCT FROM ? "
            f"OR {invalid} LIMIT 1",
            (batch_id,),
        ):
            raise StagingValidationError(f"{table}: invalid loaded row")
        keys = ", ".join(identifier(key) for key in contract["key"])
        if db.query(
            f"SELECT 1 FROM {target} GROUP BY {keys} HAVING count(*) > 1 LIMIT 1"
        ):
            raise StagingValidationError(f"{table}: duplicate loaded key")
        for parent, columns in contract["references"].items():
            join = " AND ".join(
                f"child.{identifier(c)} = parent.{identifier(c)}" for c in columns
            )
            if db.query(
                f"SELECT 1 FROM {target} child WHERE NOT EXISTS "
                f"(SELECT 1 FROM {relation(staging[parent])} parent WHERE {join}) LIMIT 1"
            ):
                raise StagingValidationError(f"{table}: loaded orphan row")


def replace_batch(db, staging, tables, batch_id, counts):
    # Fixed lock order serializes publishers, including two attempts of the same batch.
    targets = ", ".join(relation(c["target"]) for c in LOAD_CONTRACT.values())
    db.execute(f"LOCK TABLE {targets} IN SHARE ROW EXCLUSIVE MODE")
    metadata = target_schemas(db)
    for table, df in tables.items():
        validate_target_schema(table, df.schema, metadata[table])
    for contract in reversed(list(LOAD_CONTRACT.values())):
        db.execute(
            f"DELETE FROM {relation(contract['target'])} WHERE batch_id = ?",
            (batch_id,),
        )
    for table, contract in LOAD_CONTRACT.items():
        columns = ", ".join(identifier(name) for name in tables[table].columns)
        target = relation(contract["target"])
        db.execute(
            f"INSERT INTO {target} ({columns}) "
            f"SELECT {columns} FROM {relation(staging[table])}"
        )
        rows = db.query(
            f"SELECT count(*) AS rows FROM {target} WHERE batch_id = ?", (batch_id,)
        )[0]["rows"]
        if rows != counts[table]:
            raise StagingValidationError(
                f"{table}: published row count differs from manifest"
            )
    db.commit()


def load_batch(db, tables, settings, batch_id, counts):
    token = uuid4().hex
    staging = {table: f"stg._load_{token}_{table}" for table in LOAD_CONTRACT}
    try:
        for table, contract in LOAD_CONTRACT.items():
            db.execute(
                f"CREATE TABLE {relation(staging[table])} "
                f"(LIKE {relation(contract['target'])} INCLUDING DEFAULTS) DISTRIBUTED BY (id)"
            )
        db.commit()  # Executor connections must see the private loading tables.
        for table, df in tables.items():
            write_jdbc(df, staging[table], settings)
        verify_loaded(db, staging, batch_id, counts)
        replace_batch(db, staging, tables, batch_id, counts)
    except BaseException:
        db.rollback()
        raise
    finally:
        try:
            for target in staging.values():
                db.execute(f"DROP TABLE IF EXISTS {relation(target)}")
            db.commit()
        except Exception:
            db.rollback()
            logger.warning(
                "Could not remove loading tables: %s",
                list(staging.values()),
                exc_info=True,
            )
