import json

from kinopoisk_staging import run
from kinopoisk_stg_support import cloudberry_settings, parse_args
from pyspark.sql import SparkSession


def main():
    args = parse_args()
    settings = cloudberry_settings()
    spark = (
        SparkSession.builder.appName("kinopoisk_cleaned_to_stg")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        print(json.dumps(run(spark, args.manifest, settings), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
