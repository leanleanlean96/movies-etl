import json
from collections.abc import Mapping
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from .exceptions import ReconciliationError


def read_bytes(spark: SparkSession, uri: str) -> bytes:
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = fs.open(path)
    try:
        return bytes(jvm.org.apache.commons.io.IOUtils.toByteArray(stream))
    finally:
        stream.close()


def write_bytes(spark: SparkSession, uri: str, data: bytes) -> None:
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = fs.create(path, True)
    try:
        stream.write(bytearray(data))
    finally:
        stream.close()


def write_json(spark: SparkSession, uri: str, obj: Mapping[str, Any]) -> None:
    data = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True).encode()
    write_bytes(spark, uri, data)


def delete_path(spark: SparkSession, uri: str) -> None:
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(path) and not fs.delete(path, False):
        raise OSError(f"could not delete {uri}")


def write_table(df: DataFrame, uri: str, sort_keys: list[str]) -> None:
    df.coalesce(1).sortWithinPartitions(*sort_keys).write.mode("overwrite").parquet(uri)


def verify_table(
    spark: SparkSession, uri: str, expected: DataFrame, expected_rows: int
) -> int:
    written = spark.read.option("recursiveFileLookup", "true").parquet(uri)
    actual_schema = [(field.name, field.dataType) for field in written.schema]
    expected_schema = [(field.name, field.dataType) for field in expected.schema]
    if actual_schema != expected_schema:
        raise ReconciliationError(f"table {uri}: schema does not match")
    actual_rows = written.count()
    if actual_rows != expected_rows:
        raise ReconciliationError(
            f"table {uri}: {actual_rows} rows, expected {expected_rows}"
        )
    return actual_rows
