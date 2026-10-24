from datetime import timedelta

from airflow.sdk import CronDataIntervalTimetable, dag, task
from operators.kinopoisk_to_s3 import KinopoiskToS3Operator
from pendulum import datetime

from movies_pipeline.sources.kinopoisk import MOVIE_FIELDS


@dag(
    dag_id="kinopoisk_movies_daily",
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=datetime(2026, 10, 20, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["raw", "kinopoisk_api", "extract"],
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

    print_manifest_key(land.output)


extract_kinopoisk_api()
