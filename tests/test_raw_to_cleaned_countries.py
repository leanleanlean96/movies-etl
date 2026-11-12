from kinopoisk_cleaned.schema import (
    SCHEMA,
)
from kinopoisk_cleaned.transformations import (
    to_countries,
)
from pyspark.sql.types import LongType, StringType


def test_countries_preserves_links_and_removes_invalid_and_duplicate_elements(spark):
    df = spark.createDataFrame(
        [
            {
                "id": 1,
                "countries": [
                    {"id": 10, "name": " Россия "},
                    {"id": 20, "name": "США"},
                    {"id": 10, "name": "Россия"},
                    {"id": None, "name": "Unknown"},
                    None,
                ],
            },
            {"id": 2, "countries": [{"id": 10, "name": "Россия"}]},
            {"id": 3, "countries": []},
            {"id": 4},
            {"id": 5, "countries": [{"id": 30, "name": "  "}]},
        ],
        SCHEMA,
    )
    result = to_countries(spark, df, "test-batch")

    assert result.columns == ["id", "country_id", "country_name", "batch_id"]
    assert {tuple(row) for row in result.collect()} == {
        (1, 10, "Россия", "test-batch"),
        (1, 20, "США", "test-batch"),
        (2, 10, "Россия", "test-batch"),
        (5, 30, None, "test-batch"),
    }
    assert result.schema["id"].dataType == LongType()
    assert result.schema["country_id"].dataType == LongType()
    assert result.schema["country_name"].dataType == StringType()
    assert result.schema["batch_id"].dataType == StringType()


def test_countries_chooses_duplicate_name_deterministically(spark):
    df = spark.createDataFrame(
        [
            {
                "id": 1,
                "countries": [
                    {"id": 10, "name": None},
                    {"id": 10, "name": "Zulu"},
                    {"id": 10, "name": " Alpha "},
                    {"id": 10, "name": ""},
                ],
            }
        ],
        SCHEMA,
    )
    original = to_countries(spark, df, "test-batch").collect()
    repartitioned = to_countries(spark, df.repartition(2), "test-batch").collect()

    assert original == repartitioned
    assert [tuple(row) for row in original] == [(1, 10, "Alpha", "test-batch")]


def test_countries_handles_empty_input_with_output_schema(spark):
    result = to_countries(spark, spark.createDataFrame([], SCHEMA), "empty-batch")

    assert result.count() == 0
    assert result.columns == ["id", "country_id", "country_name", "batch_id"]
    assert result.schema["country_id"].dataType == LongType()
