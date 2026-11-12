import argparse
import json

from kinopoisk_cleaned.constants import DEFAULT_OUTPUT_BUCKET
from kinopoisk_cleaned.pipeline import run
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--manifest", required=True)
    p.add_argument("--output-bucket", default=DEFAULT_OUTPUT_BUCKET)
    p.add_argument("--input-root")
    p.add_argument("--output-root")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("kinopoisk_raw_to_cleaned").getOrCreate()
    try:
        cleaned = run(
            spark,
            args.manifest,
            output_bucket=args.output_bucket,
            input_root=args.input_root,
            output_root=args.output_root,
        )
        print(json.dumps(cleaned["stats"], sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
