import os
from datetime import timedelta

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import CronDataIntervalTimetable, dag, task
from kinopoisk_tasks import build_processing_tasks
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

    print_manifest_key(land.output) >> build_processing_tasks("land_movies")


kinopoisk_movies_daily = extract_kinopoisk_api()
