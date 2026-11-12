import os
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import CronDataIntervalTimetable, dag, task
from docker.types import Mount
from operators.kinopoisk_to_s3 import KinopoiskToS3Operator
from pendulum import datetime

from movies_pipeline.sources.kinopoisk import MOVIE_FIELDS

ALERT_RECIPIENTS = [
    address.strip()
    for address in os.getenv("AIRFLOW_ALERT_EMAIL", "").split(",")
    if address.strip()
]


@dag(
    dag_id="kinopoisk_movies_daily",
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=datetime(2026, 10, 20, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"email_on_failure": False, "email_on_retry": False},
    on_failure_callback=(
        SmtpNotifier(
            smtp_conn_id="smtp_alerts",
            to=ALERT_RECIPIENTS,
            subject="[Airflow] DAG {{ dag.dag_id }} failed",
            html_content=(
                "<h3>Ошибка DAG {{ dag.dag_id | e }}</h3>"
                "<p>Запуск: <code>{{ dag_run.run_id | e }}</code></p>"
                "<p>Интервал: {{ data_interval_start | e }} — {{ data_interval_end | e }}</p>"
                "<p>DAG завершился ошибкой после завершения попыток задач.</p>"
                '<p><a href="{{ ti.log_url | e }}">Открыть задачу в Airflow</a></p>'
            ),
        )
        if ALERT_RECIPIENTS
        else None
    ),
    tags=["raw", "cleaned", "stg", "dbt", "clickhouse", "kinopoisk_api", "spark"],
)
def extract_kinopoisk_api() -> None:
    land = KinopoiskToS3Operator(
        task_id="land_movies",
        s3_bucket="etl-raw",
        s3_prefix="kinopoisk/movies/dt={{ data_interval_start | ds }}",
        endpoint="/movie",
        api_params={"type": "movie", "selectFields": list(MOVIE_FIELDS)},
        partitions=[
            {"votes.kp": "1000-1999"},
            {"votes.kp": "2000-4999"},
            {"votes.kp": "5000-19999"},
            {"votes.kp": "20000-100000000"},
        ],
        split_key="votes.kp",
        max_pages=100,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    @task(task_id="print_manifest_key")
    def print_manifest_key(landed: dict) -> None:
        print(
            f"Manifest {landed['manifest_uri']}: batch {landed['batch_id']}, "
            f"{landed['records']} records in {landed['page_count']} pages"
        )

    cleaned = DockerOperator(
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
            "{{ ti.xcom_pull(task_ids='land_movies')['manifest_uri'] }}",
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
                "batch_id={{ ti.xcom_pull(task_ids='land_movies')['batch_id'] }}/_manifest.json"
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
    print_manifest_key(land.output) >> cleaned
    cleaned >> staging >> dbt >> serving


kinopoisk_movies_daily = extract_kinopoisk_api()
