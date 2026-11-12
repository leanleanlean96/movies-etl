import gzip
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path


def test_spark_submit_from_another_directory(tmp_path):
    movie = {
        "id": 1,
        "type": "movie",
        "updatedAt": "2026-10-25T12:00:00Z",
        "genres": [{"id": 10, "name": "Drama"}],
        "countries": [{"id": 20, "name": "France"}],
    }
    rows = [
        {**movie, "updatedAt": "2026-10-25T10:00:00Z"},
        movie,
        {**movie, "id": 2, "genres": [], "countries": []},
        {**movie, "id": None},
    ]
    data = gzip.compress(("\n".join(map(json.dumps, rows)) + "\n").encode())
    key = "raw/page.jsonl.gz"
    page = tmp_path / key
    page.parent.mkdir()
    page.write_bytes(data)
    manifest = {
        "manifest_version": 2,
        "status": "complete",
        "batch_id": "cli",
        "bucket": "etl-raw",
        "prefix": "raw",
        "checksum": {"algorithm": "sha256"},
        "records": len(rows),
        "page_count": 1,
        "pages": [
            {
                "key": key,
                "records": len(rows),
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        ],
        "params": {"type": "movie"},
        "interval_start": "2026-10-25T00:00:00Z",
        "interval_end": "2026-10-26T00:00:00Z",
        "finished_at": "2026-10-26T01:00:00Z",
    }
    manifest_path = tmp_path / "_manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    script = (
        Path(__file__).resolve().parents[1] / "spark/jobs/kinopoisk_raw_to_cleaned.py"
    )
    output = tmp_path / "cleaned"
    env = {
        **os.environ,
        "PYSPARK_PYTHON": sys.executable,
        "PYSPARK_DRIVER_PYTHON": sys.executable,
        "SPARK_LOCAL_IP": "127.0.0.1",
    }
    env.pop("PYTHONPATH", None)
    result = subprocess.run(
        [
            str(Path(sys.prefix) / "bin/spark-submit"),
            "--master",
            "local[2]",
            "--conf",
            "spark.ui.enabled=false",
            "--conf",
            "spark.sql.shuffle.partitions=2",
            "--conf",
            "spark.sql.session.timeZone=UTC",
            str(script),
            "--manifest",
            manifest_path.as_uri(),
            "--input-root",
            tmp_path.as_uri(),
            "--output-root",
            output.as_uri(),
        ],
        cwd=tmp_path,
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    stats = json.loads(result.stdout.strip().splitlines()[-1])
    assert (
        stats["read"],
        stats["movies"],
        stats["quarantined"],
        stats["duplicates"],
    ) == (
        4,
        2,
        1,
        1,
    )
    cleaned = json.loads(
        (output / "kinopoisk/_manifests/batch_id=cli/_manifest.json").read_text()
    )
    assert cleaned["stats"] == stats
    assert {name: table["rows"] for name, table in cleaned["tables"].items()} == {
        "movies": 2,
        "movie_genres": 1,
        "movie_countries": 1,
        "quarantine": 1,
    }
