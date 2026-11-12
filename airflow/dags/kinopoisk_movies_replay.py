"""Manually reprocess a persisted raw batch without calling Kinopoisk."""

import json
from datetime import timedelta

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Param, dag, task
from kinopoisk_tasks import build_processing_tasks
from pendulum import datetime

from movies_pipeline.replay import manifest_location, replay_batch


@dag(
    dag_id="kinopoisk_movies_replay",
    schedule=None,
    start_date=datetime(2026, 10, 20, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "manifest_uri": Param(
            None,
            type=["null", "string"],
            description="URI сохранённого raw-манифеста: s3://etl-raw/kinopoisk/movies/.../_manifest.json",
        ),
    },
    tags=["replay", "kinopoisk", "spark", "dbt", "clickhouse"],
)
def replay_kinopoisk_movies():
    @task(task_id="validate_manifest", execution_timeout=timedelta(minutes=2))
    def validate_manifest(params: dict):
        uri = params.get("manifest_uri")
        bucket, key = manifest_location(uri)
        manifest = json.loads(S3Hook(aws_conn_id="minio_s3").read_key(key, bucket))
        # Spark verifies the full manifest contract and page checksums before writing.
        return replay_batch(uri, manifest)

    validate_manifest() >> build_processing_tasks("validate_manifest")


kinopoisk_movies_replay = replay_kinopoisk_movies()
