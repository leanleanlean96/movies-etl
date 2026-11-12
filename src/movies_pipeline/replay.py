"""Validate replay identity without Airflow or Spark dependencies."""

import re
from datetime import datetime
from urllib.parse import urlsplit


def manifest_location(uri):
    if not isinstance(uri, str):
        raise TypeError("manifest_uri must be an s3:// URI")
    parsed = urlsplit(uri)
    if (
        parsed.scheme != "s3"
        or parsed.netloc != "etl-raw"
        or not parsed.path.startswith("/kinopoisk/movies/")
        or not parsed.path.endswith("/_manifest.json")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Expected s3://etl-raw/kinopoisk/movies/.../_manifest.json")
    return parsed.netloc, parsed.path.lstrip("/")


def replay_batch(uri, manifest):
    bucket, key = manifest_location(uri)
    if not isinstance(manifest, dict) or manifest.get("status") != "complete":
        raise ValueError("Replay requires a complete raw manifest")
    batch_id = manifest.get("batch_id")
    if not isinstance(batch_id, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.-]*", batch_id
    ):
        raise ValueError("Invalid batch_id")
    if (
        manifest.get("bucket") != bucket
        or manifest.get("prefix") != key.rsplit("/", 1)[0]
    ):
        raise ValueError("Manifest bucket/prefix does not match its URI")
    dates = {}
    for field in ("interval_start", "interval_end", "finished_at"):
        value = manifest.get(field)
        try:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be a datetime with timezone") from exc
        dates[field] = parsed
    if dates["interval_start"] >= dates["interval_end"]:
        raise ValueError("interval_start must be before interval_end")
    return {
        "manifest_uri": uri,
        "batch_id": batch_id,
        "interval_start": manifest["interval_start"],
        "interval_end": manifest["interval_end"],
        "ingested_at": manifest["finished_at"],
    }
