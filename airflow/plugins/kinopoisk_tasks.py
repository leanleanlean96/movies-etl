"""Shared processing chain for daily extraction and raw-manifest replay."""

import os
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import task
from docker.types import Mount

PROCESSING_POOL = "kinopoisk_processing"


def build_processing_tasks(source_task_id: str):
    cleaned = DockerOperator(
        pool=PROCESSING_POOL,
        task_id="clean_movies",
        image="etl-pet-spark:4.2.0",
        container_name="kinopoisk-cleaned-driver",
        entrypoint=["/opt/spark/bin/spark-submit"],
        command=[
            "--master",
            "spark://spark-master:7077",
            "--deploy-mode",
            "client",
            "--conf",
            "spark.driver.host=kinopoisk-cleaned-driver",
            "--conf",
            "spark.driver.bindAddress=0.0.0.0",
            "/workspace/spark/jobs/kinopoisk_raw_to_cleaned.py",
            "--manifest",
            "{{ ti.xcom_pull(task_ids='" + source_task_id + "')['manifest_uri'] }}",
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="etl-pet-net",
        mounts=[
            Mount(
                source=f"{os.environ['ETL_PROJECT_ROOT'].rstrip('/')}/spark/jobs",
                target="/workspace/spark/jobs",
                type="bind",
                read_only=True,
            )
        ],
        environment={"AWS_REGION": "us-east-1"},
        private_environment={
            "AWS_ACCESS_KEY_ID": os.environ["MINIO_ROOT_USER"],
            "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_ROOT_PASSWORD"],
        },
        mount_tmp_dir=False,
        auto_remove="force",
        do_xcom_push=False,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    staging = DockerOperator(
        pool=PROCESSING_POOL,
        task_id="load_movies_stg",
        image="etl-pet-spark:4.2.0",
        container_name="kinopoisk-stg-driver",
        entrypoint=["/opt/spark/bin/spark-submit"],
        command=[
            "--master",
            "spark://spark-master:7077",
            "--deploy-mode",
            "client",
            "--conf",
            "spark.driver.host=kinopoisk-stg-driver",
            "--conf",
            "spark.driver.bindAddress=0.0.0.0",
            "/workspace/spark/jobs/kinopoisk_cleaned_to_stg.py",
            "--manifest",
            (
                "s3a://etl-cleaned/kinopoisk/_manifests/"
                "batch_id={{ ti.xcom_pull(task_ids='"
                + source_task_id
                + "')['batch_id'] }}/_manifest.json"
            ),
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="etl-pet-net",
        mounts=[
            Mount(
                source=f"{os.environ['ETL_PROJECT_ROOT'].rstrip('/')}/spark/jobs",
                target="/workspace/spark/jobs",
                type="bind",
                read_only=True,
            )
        ],
        environment={
            "AWS_REGION": "us-east-1",
            "CLOUDBERRY_HOST": "cloudberry",
            "CLOUDBERRY_PORT": "7000",
            "CLOUDBERRY_DATABASE": "etl",
            "CLOUDBERRY_USER": "gpadmin",
        },
        private_environment={
            "AWS_ACCESS_KEY_ID": os.environ["MINIO_ROOT_USER"],
            "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_ROOT_PASSWORD"],
            "CLOUDBERRY_PASSWORD": os.environ["CLOUDBERRY_PASSWORD"],
        },
        mount_tmp_dir=False,
        auto_remove="force",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    dbt = DockerOperator(
        pool=PROCESSING_POOL,
        task_id="build_dbt",
        image="etl-pet-dbt:1.10.2",
        entrypoint=["python"],
        command=["scripts/build.py"],
        working_dir="/workspace/dbt",
        docker_url="unix://var/run/docker.sock",
        network_mode="etl-pet-net",
        mounts=[
            Mount(
                source=f"{os.environ['ETL_PROJECT_ROOT'].rstrip('/')}/dbt",
                target="/workspace/dbt",
                type="bind",
                read_only=True,
            ),
            Mount(
                source=f"{os.environ['ETL_PROJECT_ROOT'].rstrip('/')}/infra/dbt/profiles",
                target="/workspace/profiles",
                type="bind",
                read_only=True,
            ),
        ],
        environment={
            "DBT_PROFILES_DIR": "/workspace/profiles",
            "DBT_TARGET_PATH": "/tmp/dbt-target",
            "DBT_LOG_PATH": "/tmp/dbt-target/logs",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "PYTHONUNBUFFERED": "1",
            "CLOUDBERRY_HOST": "cloudberry",
            "CLOUDBERRY_PORT": "7000",
            "CLOUDBERRY_DATABASE": "etl",
            "CLOUDBERRY_USER": "gpadmin",
        },
        private_environment={
            "CLOUDBERRY_PASSWORD": os.environ["CLOUDBERRY_PASSWORD"],
        },
        mount_tmp_dir=False,
        auto_remove="force",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=30),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    @task(
        pool=PROCESSING_POOL,
        task_id="publish_clickhouse",
        execution_timeout=timedelta(minutes=30),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    def refresh_clickhouse_marts() -> None:
        # Airflow only sends control statements; ClickHouse reads Cloudberry itself.
        import logging
        from pathlib import Path

        import requests

        log = logging.getLogger(__name__)
        sql_root = Path("/opt/airflow/clickhouse")
        statements = (sql_root / "refresh.sql").read_text().split(";")
        cleanup = (sql_root / "stop.sql").read_text().split(";")
        with requests.Session() as session:
            session.auth = (
                os.environ["CLICKHOUSE_USER"],
                os.environ["CLICKHOUSE_PASSWORD"],
            )

            def execute(statement: str) -> None:
                statement = statement.strip()
                if not statement:
                    return
                response = session.post(
                    "http://clickhouse:8123/",
                    params={"wait_end_of_query": "1"},
                    data=statement.encode(),
                    timeout=(10, 1250),
                )
                if not response.ok:
                    raise RuntimeError(
                        f"ClickHouse failed: {statement}: {response.text}"
                    )
                log.info("ClickHouse: %s", statement)

            cleanup_error = None
            try:
                for statement in statements:
                    execute(statement)
            finally:
                # Also cancel an active refresh after an error or task termination.
                for statement in cleanup:
                    try:
                        execute(statement)
                    except Exception as error:
                        cleanup_error = error
                        log.exception("Could not disable ClickHouse refresh")
            if cleanup_error is not None:
                raise RuntimeError(
                    "Could not disable ClickHouse refresh"
                ) from cleanup_error

    serving = refresh_clickhouse_marts()
    cleaned >> staging >> dbt >> serving

    return cleaned
