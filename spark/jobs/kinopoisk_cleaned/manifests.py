import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from .constants import (
    CHECKSUM_ALGORITHM,
    CLEANED_MANIFEST_VERSION,
    MANIFEST_VERSION,
    SHA256_RE,
    TABLE_STATS,
)
from .exceptions import ManifestError, PageIntegrityError, ReconciliationError
from .paths import check_batch_id, cleaned_table_uri, object_key, page_uris
from .statistics import reconcile_totals
from .storage import read_bytes


def is_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def is_text(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


def check_page(index: int, page: Any, prefix: str) -> list[str]:
    where = f"pages[{index}]"
    if not isinstance(page, dict):
        return [f"{where} is not an object"]
    problems: list[str] = []
    key = page.get("key")
    if not is_text(key):
        problems.append(f"{where}.key is missing")
    elif not key.startswith(f"{prefix}/"):
        problems.append(f"{where}.key {key!r} is outside prefix {prefix!r}")
    records = page.get("records")
    if not is_count(records) or records == 0:
        problems.append(f"{where}.records must be a positive integer, got {records!r}")
    size = page.get("bytes")
    if not is_count(size) or size == 0:
        problems.append(f"{where}.bytes must be a positive integer, got {size!r}")
    digest = page.get(CHECKSUM_ALGORITHM)
    if not isinstance(digest, str) or not SHA256_RE.fullmatch(digest):
        problems.append(f"{where}.{CHECKSUM_ALGORITHM} is not a hex digest")
    return problems


def check_manifest(manifest: Any) -> None:
    if not isinstance(manifest, dict):
        raise ManifestError("manifest is not a JSON object")
    problems: list[str] = []

    version = manifest.get("manifest_version")
    if version != MANIFEST_VERSION:
        problems.append(f"manifest_version must be {MANIFEST_VERSION}, got {version!r}")
    status = manifest.get("status")
    if status != "complete":
        problems.append(f"status must be 'complete', got {status!r}")
    for field in ("batch_id", "bucket", "prefix"):
        if not is_text(manifest.get(field)):
            problems.append(f"{field} is missing")
    if is_text(manifest.get("batch_id")):
        try:
            check_batch_id(manifest["batch_id"])
        except ValueError as exc:
            problems.append(str(exc))
    dates = {}
    for field in ("interval_start", "interval_end", "finished_at"):
        value = manifest.get(field)
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError
            dates[field] = parsed
        except (AttributeError, TypeError, ValueError):
            problems.append(f"{field} must be an ISO datetime with timezone")
    if (
        "interval_start" in dates
        and "interval_end" in dates
        and dates["interval_start"] >= dates["interval_end"]
    ):
        problems.append("interval_start must be before interval_end")
    params = manifest.get("params")
    if not isinstance(params, dict):
        problems.append("params must be an object")
    elif params.get("type") is not None and not is_text(params["type"]):
        problems.append("params.type must be a non-empty string or null")
    checksum = manifest.get("checksum")
    algorithm = checksum.get("algorithm") if isinstance(checksum, dict) else None
    if algorithm != CHECKSUM_ALGORITHM:
        problems.append(
            f"checksum.algorithm must be {CHECKSUM_ALGORITHM!r}, got {algorithm!r}"
        )

    pages = manifest.get("pages")
    if not isinstance(pages, list):
        problems.append("pages is not a list")
        raise ManifestError("; ".join(problems))

    prefix = str(manifest.get("prefix") or "").strip("/")
    for index, page in enumerate(pages):
        problems.extend(check_page(index, page, prefix))

    keys = [page.get("key") for page in pages if isinstance(page, dict)]
    duplicates = sorted(
        {key for key in keys if keys.count(key) > 1 and key is not None}
    )
    if duplicates:
        problems.append(f"duplicate page keys: {duplicates}")

    page_count = manifest.get("page_count")
    if page_count != len(pages):
        problems.append(f"page_count={page_count!r} but {len(pages)} pages listed")

    records = manifest.get("records")
    page_records = [page.get("records") for page in pages if isinstance(page, dict)]
    if not is_count(records):
        problems.append(f"records must be a non-negative integer, got {records!r}")
    elif (
        all(is_count(value) for value in page_records) and sum(page_records) != records
    ):
        problems.append(f"records={records} but pages sum to {sum(page_records)}")

    if problems:
        raise ManifestError("; ".join(problems))


def read_manifest(spark: SparkSession, uri: str) -> dict[str, Any]:
    corrected_uri = "s3a://" + uri[5:] if uri.startswith("s3://") else uri
    manifest = json.loads(read_bytes(spark, corrected_uri))
    check_manifest(manifest)
    return manifest


def check_page_bytes(page: Mapping[str, Any], data: bytes) -> list[str]:
    problems: list[str] = []
    if len(data) != page["bytes"]:
        problems.append(
            f"{page['key']}: {len(data)} bytes, manifest says {page['bytes']}"
        )
    digest = hashlib.sha256(data).hexdigest()
    if digest != page[CHECKSUM_ALGORITHM]:
        problems.append(f"{page['key']}: sha256 mismatch")
    return problems


def verify_pages(
    spark: SparkSession, manifest: Mapping[str, Any], root: str | None = None
) -> None:
    problems: list[str] = []
    for page, uri in zip(manifest["pages"], page_uris(manifest, root), strict=True):
        problems.extend(check_page_bytes(page, read_bytes(spark, uri)))
    if problems:
        raise PageIntegrityError("; ".join(problems))


def reconcile_page_counts(
    manifest: Mapping[str, Any], counts: Mapping[str, int]
) -> None:
    expected = {page["key"]: page["records"] for page in manifest["pages"]}
    actual: dict[str, int] = {}
    for path, count in counts.items():
        key = object_key(path)
        actual[key] = actual.get(key, 0) + count

    problems: list[str] = []
    for key in sorted(expected.keys() - actual.keys()):
        problems.append(f"{key}: no rows read, manifest says {expected[key]}")
    for key in sorted(actual.keys() - expected.keys()):
        problems.append(f"{key}: {actual[key]} rows read from a file not in manifest")
    for key in sorted(expected.keys() & actual.keys()):
        if expected[key] != actual[key]:
            problems.append(
                f"{key}: {actual[key]} rows read, manifest says {expected[key]}"
            )
    total = sum(actual.values())
    if total != manifest["records"]:
        problems.append(
            f"total: {total} rows read, manifest says {manifest['records']}"
        )
    if problems:
        raise ReconciliationError("; ".join(problems))


def build_cleaned_manifest(
    *,
    raw_manifest: Mapping[str, Any],
    raw_manifest_uri: str,
    output_bucket: str,
    stats: Mapping[str, int],
    table_rows: Mapping[str, int],
    table_schemas: Mapping[str, StructType],
    started_at: datetime,
    finished_at: datetime,
    output_root: str | None = None,
) -> dict[str, Any]:
    if started_at.tzinfo is None or finished_at.tzinfo is None:
        raise ValueError("started_at and finished_at must be timezone-aware")
    reconcile_totals(stats)
    for table, counter in TABLE_STATS.items():
        if table_rows.get(table) != stats.get(counter) or table not in table_rows:
            raise ReconciliationError(
                f"table {table} has {table_rows.get(table)!r} rows, "
                f"stats say {stats.get(counter)!r}"
            )
    if set(table_rows) != set(TABLE_STATS):
        raise ReconciliationError("cleaned manifest must contain exactly four tables")
    if set(table_schemas) != set(TABLE_STATS) or any(
        not isinstance(schema, StructType) for schema in table_schemas.values()
    ):
        raise ReconciliationError("cleaned manifest must contain four table schemas")
    batch_id = raw_manifest["batch_id"]
    tables = {
        table: {
            "uri": cleaned_table_uri(output_bucket, table, batch_id, output_root),
            "rows": rows,
            "schema": table_schemas[table].jsonValue(),
        }
        for table, rows in sorted(table_rows.items())
    }
    return {
        "manifest_version": CLEANED_MANIFEST_VERSION,
        "status": "complete",
        "batch_id": batch_id,
        "source": {
            "manifest_uri": raw_manifest_uri,
            "records": raw_manifest["records"],
            "page_count": raw_manifest["page_count"],
            "ingested_at": raw_manifest.get("finished_at"),
            "interval_start": raw_manifest["interval_start"],
            "interval_end": raw_manifest["interval_end"],
            "type": raw_manifest["params"].get("type"),
        },
        "stats": dict(sorted(stats.items())),
        "tables": tables,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
    }
