import hashlib
import json
import logging
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from movies_pipeline.storage.s3 import (
    GZIP_CONTENT_TYPE,
    put_bytes,
    put_json,
    to_jsonl_gz,
)

logger = logging.getLogger(__name__)

MANIFEST_NAME = "_manifest.json"
MANIFEST_VERSION = 2
CHECKSUM_ALGORITHM = "sha256"

LabeledPage = tuple[Mapping[str, Any], Mapping[str, Any]]


def page_key(prefix: str, page_no: int) -> str:
    return f"{prefix}/page-{page_no:05d}.jsonl.gz"


def manifest_key(prefix: str) -> str:
    return f"{prefix}/{MANIFEST_NAME}"


def s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def _partition_id(partition: Mapping[str, Any]) -> str:
    return json.dumps(partition, sort_keys=True, ensure_ascii=False, default=str)


def extract_to_s3(
    *,
    pages: Iterable[LabeledPage],
    s3: Any,
    bucket: str,
    prefix: str,
    batch_id: str,
    meta: Mapping[str, Any],
) -> dict[str, Any]:
    prefix = prefix.strip("/")
    if not prefix:
        raise ValueError("prefix must not be empty")
    if not batch_id:
        raise ValueError("batch_id must not be empty")

    started_at = datetime.now(UTC)
    page_entries: list[dict[str, Any]] = []
    partition_stats: dict[str, dict[str, Any]] = {}
    empty_pages = 0

    for partition, page in pages:
        stats = partition_stats.setdefault(
            _partition_id(partition),
            {"partition": dict(partition), "pages": 0, "empty_pages": 0, "records": 0},
        )
        docs = page.get("docs") or []
        if not docs:
            empty_pages += 1
            stats["empty_pages"] += 1
            continue

        key = page_key(prefix, len(page_entries) + 1)
        body = to_jsonl_gz(docs)
        put_bytes(s3, bucket, key, body, content_type=GZIP_CONTENT_TYPE)
        page_entries.append(
            {
                "key": key,
                "partition": dict(partition),
                "records": len(docs),
                "bytes": len(body),
                CHECKSUM_ALGORITHM: hashlib.sha256(body).hexdigest(),
            }
        )
        stats["pages"] += 1
        stats["records"] += len(docs)
        logger.info("Landed %d records to %s", len(docs), s3_uri(bucket, key))

    records = sum(entry["records"] for entry in page_entries)
    key = manifest_key(prefix)
    put_json(
        s3,
        bucket,
        key,
        {
            **meta,
            "manifest_version": MANIFEST_VERSION,
            "status": "complete",
            "batch_id": batch_id,
            "bucket": bucket,
            "prefix": prefix,
            "checksum": {"algorithm": CHECKSUM_ALGORITHM, "scope": "stored_object"},
            "page_count": len(page_entries),
            "empty_pages": empty_pages,
            "records": records,
            "partition_stats": list(partition_stats.values()),
            "pages": page_entries,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
        },
    )
    uri = s3_uri(bucket, key)
    logger.info("Manifest %s: %d records in %d pages", uri, records, len(page_entries))
    return {
        "manifest_uri": uri,
        "batch_id": batch_id,
        "page_count": len(page_entries),
        "records": records,
    }
