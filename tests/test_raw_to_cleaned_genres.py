from kinopoisk_cleaned.schema import (
    SCHEMA,
)
from kinopoisk_cleaned.transformations import (
    to_genres,
)
from pyspark.sql.types import LongType, StringType


def test_genres_cleans_metadata_and_preserves_unique_valid_links(spark):
    df = spark.createDataFrame(
        [
            {
                "id": 1,
                "genres": [
                    {"id": 10, "name": " Драма ", "slug": " drama "},
                    {"id": 20, "name": "Триллер", "slug": "thriller"},
                    {"id": 10, "name": "Драма", "slug": "drama"},
                    {"id": None, "name": "Unknown", "slug": "unknown"},
                    None,
                ],
            },
            {"id": 2, "genres": [{"id": 10, "name": "Драма", "slug": "drama"}]},
            {"id": 3, "genres": []},
            {"id": 4},
            {"id": 5, "genres": [{"id": 30, "name": "  ", "slug": ""}]},
        ],
        SCHEMA,
    )
    result = to_genres(spark, df, "test-batch")

    assert result.columns == ["id", "genre_id", "genre_name", "genre_slug", "batch_id"]
    assert {tuple(row) for row in result.collect()} == {
        (1, 10, "Драма", "drama", "test-batch"),
        (1, 20, "Триллер", "thriller", "test-batch"),
        (2, 10, "Драма", "drama", "test-batch"),
        (5, 30, None, None, "test-batch"),
    }
    assert result.schema["id"].dataType == LongType()
    assert result.schema["genre_id"].dataType == LongType()
    for column in ("genre_name", "genre_slug", "batch_id"):
        assert result.schema[column].dataType == StringType()


def test_genres_chooses_duplicate_metadata_deterministically(spark):
    elements = [
        {"id": 10, "name": None, "slug": None},
        {"id": 10, "name": "Zulu", "slug": "zulu"},
        {"id": 10, "name": "Alpha", "slug": None},
        {"id": 10, "name": " Alpha ", "slug": " b "},
        {"id": 10, "name": "Alpha", "slug": " a "},
        {"id": 10, "name": "", "slug": "x"},
    ]
    df = spark.createDataFrame([{"id": 1, "genres": elements}], SCHEMA)
    reordered = spark.createDataFrame(
        [{"id": 1, "genres": list(reversed(elements))}], SCHEMA
    ).repartition(2)

    original = to_genres(spark, df, "test-batch").collect()
    reversed_result = to_genres(spark, reordered, "test-batch").collect()

    assert original == reversed_result
    assert [tuple(row) for row in original] == [(1, 10, "Alpha", "a", "test-batch")]


def test_genres_handles_empty_input_with_output_schema(spark):
    result = to_genres(spark, spark.createDataFrame([], SCHEMA), "empty-batch")

    assert result.count() == 0
    assert result.columns == ["id", "genre_id", "genre_name", "genre_slug", "batch_id"]
    assert result.schema["genre_id"].dataType == LongType()
