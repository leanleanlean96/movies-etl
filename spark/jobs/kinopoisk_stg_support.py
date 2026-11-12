import argparse
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote, unquote, urlparse

from kinopoisk_cleaned.constants import CLEANED_MANIFEST_VERSION, TABLE_STATS
from kinopoisk_cleaned.paths import check_batch_id
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructType,
    TimestampNTZType,
    TimestampType,
)

LOAD_CONTRACT = {
    "movies": {
        "target": "stg.movies",
        "key": ("batch_id", "id"),
        "required": ("source_updated_at", "ingested_at"),
        "references": {},
    },
    "movie_genres": {
        "target": "stg.movie_genres",
        "key": ("batch_id", "id", "genre_id"),
        "required": (),
        "references": {"movies": ("batch_id", "id")},
    },
    "movie_countries": {
        "target": "stg.movie_countries",
        "key": ("batch_id", "id", "country_id"),
        "required": (),
        "references": {"movies": ("batch_id", "id")},
    },
}

SPARK_SQL_TYPES = {
    BooleanType: {"boolean"},
    ByteType: {"smallint", "integer", "bigint"},
    ShortType: {"smallint", "integer", "bigint"},
    IntegerType: {"integer", "bigint"},
    LongType: {"bigint"},
    FloatType: {"real", "double precision"},
    DoubleType: {"double precision"},
    StringType: {"text", "character varying"},
    DateType: {"date"},
    TimestampType: {"timestamp with time zone"},
    TimestampNTZType: {"timestamp without time zone"},
    BinaryType: {"bytea"},
}
INTEGER_DIGITS = {ByteType: 3, ShortType: 5, IntegerType: 10, LongType: 19}


class StagingValidationError(ValueError):
    pass


@dataclass(frozen=True)
class CloudberrySettings:
    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.host}:{self.port}/{quote(self.database, safe='')}"
        )


def cloudberry_settings(
    environ: Mapping[str, str] | None = None,
) -> CloudberrySettings:
    env = os.environ if environ is None else environ
    values = {}
    for name in ("CLOUDBERRY_HOST", "CLOUDBERRY_USER", "CLOUDBERRY_PASSWORD"):
        value = env.get(name)
        if not value or not value.strip():
            raise ValueError(f"Missing environment variable: {name}")
        values[name] = value
    host = values["CLOUDBERRY_HOST"]
    if any(char.isspace() or char in "/?#@|" for char in host):
        raise ValueError("Invalid CLOUDBERRY_HOST")
    try:
        port = int(env.get("CLOUDBERRY_PORT", "7000"))
    except ValueError:
        raise ValueError("CLOUDBERRY_PORT must be an integer") from None
    if not 1 <= port <= 65535:
        raise ValueError("CLOUDBERRY_PORT must be between 1 and 65535")
    database = env.get("CLOUDBERRY_DATABASE", "etl")
    if not database.strip():
        raise ValueError("CLOUDBERRY_DATABASE must not be empty")
    return CloudberrySettings(
        host=host,
        port=port,
        database=database,
        user=values["CLOUDBERRY_USER"],
        password=values["CLOUDBERRY_PASSWORD"],
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load a cleaned batch into stg")
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args(argv)
    try:
        _s3_path(args.manifest)
    except StagingValidationError as exc:
        parser.error(str(exc))
    return args


def _s3_path(uri: Any) -> tuple[str, str]:
    if not isinstance(uri, str):
        raise StagingValidationError("Expected an s3:// or s3a:// URI")
    parsed = urlparse(uri)
    path = unquote(parsed.path).lstrip("/")
    if (
        parsed.scheme not in {"s3", "s3a"}
        or not parsed.netloc
        or not path
        or parsed.username is not None
        or parsed.query
        or parsed.fragment
        or any(part in {".", ".."} for part in path.split("/"))
    ):
        raise StagingValidationError("Expected a plain s3:// or s3a:// object URI")
    return parsed.netloc, path


def _count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def validate_cleaned_manifest(manifest: Any) -> None:
    if not isinstance(manifest, dict):
        raise StagingValidationError("Cleaned manifest must be a JSON object")
    if (
        type(manifest.get("manifest_version")) is not int
        or manifest.get("manifest_version") != CLEANED_MANIFEST_VERSION
    ):
        raise StagingValidationError("Unsupported cleaned manifest version")
    if manifest.get("status") != "complete":
        raise StagingValidationError("Cleaned manifest is not complete")
    batch_id = manifest.get("batch_id")
    if not isinstance(batch_id, str):
        raise StagingValidationError("Missing batch_id")
    try:
        check_batch_id(batch_id)
    except ValueError as exc:
        raise StagingValidationError(str(exc)) from exc
    tables, stats, source = (
        manifest.get("tables"),
        manifest.get("stats"),
        manifest.get("source"),
    )
    if not isinstance(tables, dict) or set(tables) != set(TABLE_STATS):
        raise StagingValidationError("Expected exactly four cleaned tables")
    if not isinstance(stats, dict) or not isinstance(source, dict):
        raise StagingValidationError("Missing stats or source")
    buckets = set()
    schemas_present = []
    for table, counter in TABLE_STATS.items():
        entry = tables[table]
        if not isinstance(entry, dict) or not _count(entry.get("rows")):
            raise StagingValidationError(f"{table}: invalid row count")
        bucket, path = _s3_path(entry.get("uri"))
        if path != f"kinopoisk/{table}/batch_id={batch_id}":
            raise StagingValidationError(f"{table}: URI does not match batch_id/table")
        buckets.add(bucket)
        if not _count(stats.get(counter)) or entry["rows"] != stats[counter]:
            raise StagingValidationError(f"{table}: rows disagree with stats")
        schemas_present.append("schema" in entry)
        if "schema" in entry:
            manifest_table_schema(manifest, table)
    if any(schemas_present) and not all(schemas_present):
        raise StagingValidationError("Schemas must be supplied for all four tables")
    if len(buckets) != 1:
        raise StagingValidationError("Cleaned table URIs must use one bucket")
    for counter in ("read", "duplicates"):
        if not _count(stats.get(counter)):
            raise StagingValidationError(f"Invalid stats.{counter}")
    if stats["read"] != stats["movies"] + stats["quarantined"] + stats["duplicates"]:
        raise StagingValidationError("read != movies + quarantined + duplicates")
    if not _count(source.get("records")) or source["records"] != stats["read"]:
        raise StagingValidationError("source.records disagrees with stats.read")


def manifest_table_schema(manifest: Mapping[str, Any], table: str) -> StructType:
    try:
        value = manifest["tables"][table]["schema"]
        if not isinstance(value, dict) or value.get("type") != "struct":
            raise ValueError
        if not isinstance(value.get("fields"), list):
            raise TypeError
        schema = StructType.fromJson(value)
        names = schema.fieldNames()
        if not names or any(not isinstance(name, str) or not name for name in names):
            raise ValueError
        if len(set(names)) != len(names):
            raise ValueError
        return schema
    except (KeyError, TypeError, ValueError, AttributeError, AssertionError):
        raise StagingValidationError(
            f"{table}: invalid or missing manifest schema"
        ) from None


def validate_table_schema(
    table: str, schema: StructType, declared: StructType | None = None
) -> None:
    if table not in LOAD_CONTRACT:
        raise StagingValidationError(f"Unknown load table: {table}")
    actual = {field.name: field.dataType for field in schema}
    required = set(LOAD_CONTRACT[table]["key"]) | set(LOAD_CONTRACT[table]["required"])
    if len(actual) != len(schema) or not required.issubset(actual):
        raise StagingValidationError(f"{table}: duplicate or missing required columns")
    if declared is not None:
        expected = {field.name: field.dataType for field in declared}
        if len(expected) != len(declared) or actual != expected:
            raise StagingValidationError(f"{table}: schema differs from manifest")


def manifest_table_columns(manifest: Mapping[str, Any], table: str) -> tuple[str, ...]:
    return tuple(manifest_table_schema(manifest, table).fieldNames())


def _target_columns(
    columns: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    result = {}
    for column in columns:
        name = column.get("column_name")
        if (
            not isinstance(name, str)
            or not name
            or name in result
            or not isinstance(column.get("data_type"), str)
            or column.get("is_nullable") not in ("YES", "NO")
        ):
            raise StagingValidationError("Invalid target column metadata")
        result[name] = column
    if not result:
        raise StagingValidationError("Target table has no column metadata")
    return result


def _compatible_type(kind: Any, column: Mapping[str, Any]) -> bool:
    target = column["data_type"].lower()
    if target == "numeric":
        if "numeric_precision" not in column or "numeric_scale" not in column:
            return False
        if isinstance(kind, DecimalType):
            digits, scale = kind.precision - kind.scale, kind.scale
        elif type(kind) in INTEGER_DIGITS:
            digits, scale = INTEGER_DIGITS[type(kind)], 0
        else:
            return False
        precision = column.get("numeric_precision")
        target_scale = column.get("numeric_scale")
        if precision is None and target_scale is None:
            return True
        return (
            isinstance(precision, int)
            and isinstance(target_scale, int)
            and precision - target_scale >= digits
            and target_scale >= scale
        )
    if target not in SPARK_SQL_TYPES.get(type(kind), set()):
        return False
    return not (
        isinstance(kind, StringType)
        and target == "character varying"
        and (
            "character_maximum_length" not in column
            or column["character_maximum_length"] is not None
        )
    )


def validate_target_schema(
    table: str, schema: StructType, target_columns: Sequence[Mapping[str, Any]]
) -> None:
    validate_table_schema(table, schema)
    target = _target_columns(target_columns)
    for source_column in schema:
        column = target.get(source_column.name)
        if column is None:
            raise StagingValidationError(
                f"{table}: target lacks column {source_column.name}"
            )
        if column.get("is_generated", "NEVER") != "NEVER":
            raise StagingValidationError(
                f"{table}.{source_column.name}: target column is generated"
            )
        if column.get("identity_generation") == "ALWAYS":
            raise StagingValidationError(
                f"{table}.{source_column.name}: target identity is ALWAYS"
            )
        if not _compatible_type(source_column.dataType, column):
            raise StagingValidationError(
                f"{table}.{source_column.name}: {source_column.dataType.simpleString()} is incompatible "
                f"with target {column['data_type']}"
            )
    for name in target.keys() - set(schema.fieldNames()):
        column = target[name]
        if (
            column["is_nullable"] == "NO"
            and column.get("column_default") is None
            and column.get("is_generated", "NEVER") == "NEVER"
            and column.get("is_identity", "NO") != "YES"
        ):
            raise StagingValidationError(
                f"{table}: missing required target column {name}"
            )


def validate_tables(
    tables: Mapping[str, DataFrame],
    manifest: Mapping[str, Any],
    target_schemas: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
) -> dict[str, int]:
    validate_cleaned_manifest(manifest)
    if set(tables) != set(LOAD_CONTRACT):
        raise StagingValidationError(f"Expected load tables: {sorted(LOAD_CONTRACT)}")
    if target_schemas is not None and set(target_schemas) != set(LOAD_CONTRACT):
        raise StagingValidationError("Expected target metadata for every load table")
    counts = {}
    for table, contract in LOAD_CONTRACT.items():
        df = tables[table]
        declared = (
            manifest_table_schema(manifest, table)
            if "schema" in manifest["tables"][table]
            else None
        )
        validate_table_schema(table, df.schema, declared)
        if df.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
            raise StagingValidationError("Spark session timezone must be UTC")
        required = set(contract["key"]) | set(contract["required"])
        if target_schemas is not None:
            validate_target_schema(table, df.schema, target_schemas[table])
            required.update(
                column["column_name"]
                for column in target_schemas[table]
                if column["is_nullable"] == "NO" and column["column_name"] in df.columns
            )
        invalid = ~F.col("batch_id").eqNullSafe(F.lit(manifest["batch_id"]))
        for name in required:
            invalid = invalid | F.col(name).isNull()
        result = df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.count(F.when(invalid, 1)).alias("invalid"),
        ).first()
        counts[table] = result["rows"]
        if counts[table] != manifest["tables"][table]["rows"]:
            raise StagingValidationError(f"{table}: row count differs from manifest")
        if result["invalid"]:
            raise StagingValidationError(
                f"{table}: wrong batch_id or NULL required field"
            )
        if df.groupBy(*contract["key"]).count().where("count > 1").limit(1).count():
            raise StagingValidationError(f"{table}: duplicate key")
    for table, contract in LOAD_CONTRACT.items():
        for parent, columns in contract["references"].items():
            parent_keys = tables[parent].select(*columns)
            orphans = tables[table].join(parent_keys, list(columns), "left_anti")
            if orphans.limit(1).count():
                raise StagingValidationError(
                    f"{table}: referenced row is absent from {parent}"
                )
    return counts
