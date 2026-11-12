from collections.abc import Mapping
from typing import Any
from urllib.parse import unquote, urlparse

from .constants import CLEANED_SOURCE, TABLE_NAME_RE


def object_key(uri_or_key: str) -> str:
    if "://" not in uri_or_key:
        return uri_or_key.strip("/")
    parsed = urlparse(uri_or_key)
    if parsed.scheme not in {"s3", "s3a"}:
        raise ValueError(f"not an S3 URI: {uri_or_key!r}")
    return unquote(parsed.path).lstrip("/")


def page_uris(manifest: Mapping[str, Any], root: str | None = None) -> list[str]:
    bucket = manifest["bucket"]
    root = (root or f"s3a://{bucket}").rstrip("/")
    return [f"{root}/{page['key']}" for page in manifest["pages"]]


def page_numbers(manifest: Mapping[str, Any]) -> dict[str, int]:
    return {
        page["key"]: number for number, page in enumerate(manifest["pages"], start=1)
    }


def check_batch_id(batch_id: str) -> None:
    if (
        not batch_id
        or batch_id in {".", ".."}
        or "/" in batch_id
        or "\\" in batch_id
        or batch_id != batch_id.strip()
    ):
        raise ValueError(f"bad batch_id: {batch_id!r}")


def cleaned_table_uri(
    bucket: str, table: str, batch_id: str, root: str | None = None
) -> str:
    if not TABLE_NAME_RE.fullmatch(table):
        raise ValueError(f"bad table name: {table!r}")
    check_batch_id(batch_id)
    root = (root or f"s3a://{bucket}").rstrip("/")
    return f"{root}/{CLEANED_SOURCE}/{table}/batch_id={batch_id}"


def cleaned_manifest_uri(bucket: str, batch_id: str, root: str | None = None) -> str:
    check_batch_id(batch_id)
    root = (root or f"s3a://{bucket}").rstrip("/")
    return f"{root}/{CLEANED_SOURCE}/_manifests/batch_id={batch_id}/_manifest.json"
