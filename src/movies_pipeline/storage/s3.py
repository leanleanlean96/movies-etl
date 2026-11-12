import gzip
import json
from collections.abc import Iterable, Mapping
from typing import Any

GZIP_CONTENT_TYPE = "application/gzip"
JSON_CONTENT_TYPE = "application/json"


def to_jsonl_gz(records: Iterable[Mapping[str, Any]]) -> bytes:
    lines = "".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        for record in records
    )
    return gzip.compress(lines.encode("utf-8"))


def put_bytes(
    s3: Any, bucket: str, key: str, data: bytes, *, content_type: str
) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)


def put_json(s3: Any, bucket: str, key: str, obj: Any) -> None:
    body = json.dumps(obj, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    put_bytes(s3, bucket, key, body, content_type=JSON_CONTENT_TYPE)
