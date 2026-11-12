import json
import logging

from kinopoisk_cleaned.storage import read_bytes
from kinopoisk_stg_jdbc import JdbcConnection, load_batch, target_schemas
from kinopoisk_stg_support import (
    LOAD_CONTRACT,
    _s3_path,
    cloudberry_settings,
    validate_cleaned_manifest,
    validate_tables,
)

logger = logging.getLogger(__name__)


def s3a_uri(uri):
    _s3_path(uri)
    return "s3a://" + uri.split("://", 1)[1]


def run(spark, manifest_uri, settings=None):
    settings = settings or cloudberry_settings()
    manifest = json.loads(read_bytes(spark, s3a_uri(manifest_uri)))
    validate_cleaned_manifest(manifest)
    tables = {}
    db = None
    try:
        for table in LOAD_CONTRACT:
            tables[table] = (
                spark.read.option("recursiveFileLookup", "true")
                .parquet(s3a_uri(manifest["tables"][table]["uri"]))
                .persist()
            )
        db = JdbcConnection(spark, settings)
        counts = validate_tables(tables, manifest, target_schemas(db))
        db.commit()
        logger.info("Loading batch %s: %s", manifest["batch_id"], counts)
        load_batch(db, tables, settings, manifest["batch_id"], counts)
        return {
            "batch_id": manifest["batch_id"],
            "status": "complete",
            "tables": counts,
        }
    finally:
        if db is not None:
            db.close()
        for df in tables.values():
            df.unpersist()
