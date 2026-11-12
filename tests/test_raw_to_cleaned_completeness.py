import json
from datetime import UTC, datetime

from kinopoisk_cleaned.schema import SCHEMA
from kinopoisk_cleaned.transformations import to_movies
from kinopoisk_stg_support import validate_table_schema, validate_target_schema
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


def test_completeness_survives_json_parquet_and_staging_validation(spark, tmp_path):
    documents = [
        {"id": 1, "genres": [], "countries": []},
        {"id": 2, "genres": [{"id": 10}], "countries": [{"id": 20}]},
        {"id": 3, "genres": None, "countries": None},
        {"id": 4},
        {"id": 5, "genres": [None], "countries": [{"name": "Unknown"}]},
        {"id": 6, "genres": [], "countries": None},
        {"id": 7, "genres": None, "countries": []},
        {"id": 8, "genres": [{"id": 10}, None], "countries": [{"id": 20}]},
    ]
    source = tmp_path / "raw.jsonl"
    source.write_text("\n".join(json.dumps(doc) for doc in documents))
    now = datetime(2026, 11, 12, tzinfo=UTC)
    raw = (
        spark.read.schema(SCHEMA)
        .json(str(source))
        .withColumn("source_updated_at", F.lit(now))
        .withColumn("source_page", F.lit(1))
    )
    movies = to_movies(spark, raw, batch_id="completeness", ingested_at=now)
    output = str(tmp_path / "movies")
    movies.write.parquet(output)
    written = spark.read.parquet(output)
    assert {
        row.id: (row.genres_complete, row.countries_complete)
        for row in written.select(
            "id", "genres_complete", "countries_complete"
        ).collect()
    } == {
        1: (True, True),
        2: (True, True),
        3: (False, False),
        4: (False, False),
        5: (False, False),
        6: (True, False),
        7: (False, True),
        8: (False, True),
    }
    for field in ("genres_complete", "countries_complete"):
        assert written.schema[field].dataType == BooleanType()
    validate_table_schema("movies", written.schema, movies.schema)
    # The loader accepts and forwards additional boolean columns by name.
    flags = written.select(
        "id",
        "batch_id",
        "source_updated_at",
        "ingested_at",
        "genres_complete",
        "countries_complete",
    )
    sql_types = {
        "id": "bigint",
        "batch_id": "text",
        "source_updated_at": "timestamp with time zone",
        "ingested_at": "timestamp with time zone",
        "genres_complete": "boolean",
        "countries_complete": "boolean",
    }
    validate_target_schema(
        "movies",
        flags.schema,
        [
            {"column_name": name, "data_type": sql_types[name], "is_nullable": "YES"}
            for name in flags.columns
        ],
    )
