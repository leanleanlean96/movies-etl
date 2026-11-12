from datetime import UTC, datetime

import pytest
from kinopoisk_stg_support import (
    LOAD_CONTRACT,
    CloudberrySettings,
    StagingValidationError,
    cloudberry_settings,
    manifest_table_columns,
    manifest_table_schema,
    parse_args,
    validate_cleaned_manifest,
    validate_table_schema,
    validate_tables,
    validate_target_schema,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TEST_SCHEMAS = {
    "movies": StructType(
        [
            StructField("id", LongType()),
            StructField("batch_id", StringType()),
            StructField("source_updated_at", TimestampType()),
            StructField("ingested_at", TimestampType()),
            StructField("name", StringType()),
        ]
    ),
    "movie_genres": StructType(
        [
            StructField("id", LongType()),
            StructField("batch_id", StringType()),
            StructField("genre_id", LongType()),
            StructField("genre_name", StringType()),
            StructField("genre_slug", StringType()),
        ]
    ),
    "movie_countries": StructType(
        [
            StructField("id", LongType()),
            StructField("batch_id", StringType()),
            StructField("country_id", LongType()),
        ]
    ),
}


def manifest(movies=1, genres=1, countries=0):
    sizes = {
        "movies": movies,
        "movie_genres": genres,
        "movie_countries": countries,
        "quarantine": 0,
    }
    return {
        "manifest_version": 1,
        "status": "complete",
        "batch_id": "demo",
        "source": {"records": movies},
        "stats": {
            "read": movies,
            "movies": movies,
            "genre_links": genres,
            "country_links": countries,
            "quarantined": 0,
            "duplicates": 0,
        },
        "tables": {
            table: {
                "rows": size,
                "uri": f"s3a://etl-cleaned/kinopoisk/{table}/batch_id=demo",
            }
            for table, size in sizes.items()
        },
    }


def frames(spark, *, movie_overrides=None, genre_overrides=None, duplicate=False):
    now = datetime(2026, 11, 1, tzinfo=UTC)
    movie = dict.fromkeys(TEST_SCHEMAS["movies"].fieldNames())
    movie.update(id=1, batch_id="demo", source_updated_at=now, ingested_at=now)
    movie.update(movie_overrides or {})
    genre = {
        "id": 1,
        "genre_id": 5,
        "genre_name": None,
        "genre_slug": None,
        "batch_id": "demo",
    }
    genre.update(genre_overrides or {})
    return {
        "movies": spark.createDataFrame([movie], TEST_SCHEMAS["movies"]),
        "movie_genres": spark.createDataFrame(
            [genre, genre] if duplicate else [genre], TEST_SCHEMAS["movie_genres"]
        ),
        "movie_countries": spark.createDataFrame([], TEST_SCHEMAS["movie_countries"]),
    }


def test_environment_and_secret_redaction():
    settings = cloudberry_settings(
        {
            "CLOUDBERRY_HOST": "cloudberry",
            "CLOUDBERRY_USER": "gpadmin",
            "CLOUDBERRY_PASSWORD": "example-secret",
        }
    )
    assert settings.jdbc_url == "jdbc:postgresql://cloudberry:7000/etl"
    assert settings.password == "example-secret"
    assert "example-secret" not in repr(settings)
    assert "example-secret" not in settings.jdbc_url


@pytest.mark.parametrize("port", ["abc", "0", "65536"])
def test_bad_port(port):
    with pytest.raises(ValueError, match="CLOUDBERRY_PORT"):
        cloudberry_settings(
            {
                "CLOUDBERRY_HOST": "cloudberry",
                "CLOUDBERRY_USER": "gpadmin",
                "CLOUDBERRY_PASSWORD": "secret",
                "CLOUDBERRY_PORT": port,
            }
        )


def test_missing_secret_is_not_filled_with_default():
    with pytest.raises(ValueError, match="CLOUDBERRY_PASSWORD"):
        cloudberry_settings(
            {"CLOUDBERRY_HOST": "localhost", "CLOUDBERRY_USER": "gpadmin"}
        )


def test_database_is_url_encoded():
    settings = CloudberrySettings("localhost", 7000, "my db?x", "gpadmin", "secret")
    assert settings.jdbc_url.endswith("/my%20db%3Fx")


def test_parse_args_does_not_run_job():
    uri = "s3://etl-cleaned/kinopoisk/_manifests/batch_id=demo/_manifest.json"
    assert parse_args(["--manifest", uri]).manifest == uri
    with pytest.raises(SystemExit):
        parse_args(["--manifest", "https://example.com/data"])


def test_valid_manifest_and_empty_batch():
    validate_cleaned_manifest(manifest())
    validate_cleaned_manifest(manifest(0, 0, 0))


def manifest_with_schemas():
    value = manifest()
    for table, entry in value["tables"].items():
        schema = TEST_SCHEMAS.get(table, StructType([StructField("id", LongType())]))
        entry["schema"] = schema.jsonValue()
    return value


def test_manifest_schema_roundtrip():
    value = manifest_with_schemas()
    validate_cleaned_manifest(value)
    assert manifest_table_schema(value, "movies") == TEST_SCHEMAS["movies"]
    assert manifest_table_columns(value, "movies") == tuple(
        TEST_SCHEMAS["movies"].fieldNames()
    )


@pytest.mark.parametrize(
    "schema",
    [
        None,
        {},
        {"type": "struct", "fields": "id"},
        {
            "type": "struct",
            "fields": [{"name": "id", "type": "bad-type", "nullable": True}],
        },
    ],
)
def test_manifest_rejects_malformed_schema(schema):
    value = manifest_with_schemas()
    value["tables"]["movies"]["schema"] = schema
    with pytest.raises(StagingValidationError, match="manifest schema"):
        validate_cleaned_manifest(value)


def test_manifest_rejects_partial_schemas():
    value = manifest_with_schemas()
    del value["tables"]["quarantine"]["schema"]
    with pytest.raises(StagingValidationError, match="all four"):
        validate_cleaned_manifest(value)


def test_manifest_schema_comparison_ignores_nullability():
    schema = TEST_SCHEMAS["movies"]
    declared = StructType([StructField(f.name, f.dataType, False) for f in schema])
    validate_table_schema("movies", schema, declared)
    with pytest.raises(StagingValidationError, match="differs from manifest"):
        validate_table_schema("movies", schema, StructType([schema["id"]]))


@pytest.mark.parametrize(
    "change",
    [
        lambda m: m.update(status="writing"),
        lambda m: m.update(manifest_version=True),
        lambda m: m.update(batch_id="../demo"),
        lambda m: m["tables"]["movies"].update(rows=True),
        lambda m: m["stats"].update(duplicates=1),
        lambda m: m["source"].update(records=2),
        lambda m: m["tables"].pop("quarantine"),
        lambda m: m["tables"]["movies"].update(
            uri="s3a://etl-cleaned/kinopoisk/movies/batch_id=other"
        ),
        lambda m: m["tables"]["movie_genres"].update(
            uri="s3a://another-bucket/kinopoisk/movie_genres/batch_id=demo"
        ),
    ],
)
def test_manifest_rejects_inconsistent_input(change):
    value = manifest()
    change(value)
    with pytest.raises(StagingValidationError):
        validate_cleaned_manifest(value)


def test_schema_allows_order_changes_but_rejects_type_changes():
    schema = TEST_SCHEMAS["movie_genres"]
    validate_table_schema(
        "movie_genres", StructType(list(reversed(schema.fields))), schema
    )
    bad = StructType(
        [
            StructField(f.name, LongType() if f.name == "batch_id" else f.dataType)
            for f in schema
        ]
    )
    with pytest.raises(StagingValidationError, match="schema"):
        validate_table_schema("movie_genres", bad, schema)
    with pytest.raises(StagingValidationError, match="duplicate"):
        validate_table_schema(
            "movie_genres", StructType(schema.fields + [schema["id"]])
        )


def test_valid_frames_and_empty_frames(spark):
    assert validate_tables(frames(spark), manifest()) == {
        "movies": 1,
        "movie_genres": 1,
        "movie_countries": 0,
    }
    empty = {
        table: spark.createDataFrame([], schema)
        for table, schema in TEST_SCHEMAS.items()
    }
    assert validate_tables(empty, manifest(0, 0, 0)) == dict.fromkeys(empty, 0)


def test_frames_match_manifest_schema(spark):
    value = manifest_with_schemas()
    assert validate_tables(frames(spark), value)["movies"] == 1
    value["tables"]["movies"]["schema"] = StructType(
        [StructField("id", LongType())]
    ).jsonValue()
    with pytest.raises(StagingValidationError, match="differs from manifest"):
        validate_tables(frames(spark), value)


@pytest.mark.parametrize("overrides", [{"batch_id": "other"}, {"ingested_at": None}])
def test_frames_reject_bad_batch_and_required_null(spark, overrides):
    with pytest.raises(StagingValidationError, match="wrong batch_id or NULL"):
        validate_tables(frames(spark, movie_overrides=overrides), manifest())


def test_frames_reject_duplicate_and_orphan(spark):
    with pytest.raises(StagingValidationError, match="duplicate"):
        validate_tables(frames(spark, duplicate=True), manifest(genres=2))
    with pytest.raises(StagingValidationError, match="absent from movies"):
        validate_tables(frames(spark, genre_overrides={"id": 2}), manifest())


def test_frames_reject_wrong_row_count(spark):
    with pytest.raises(StagingValidationError, match="row count"):
        validate_tables(frames(spark), manifest(movies=2))


def test_schemas_match_actual_cleaned_transformations(spark):
    from kinopoisk_cleaned.schema import SCHEMA
    from kinopoisk_cleaned.transformations import to_countries, to_genres, to_movies
    from pyspark.sql import functions as F

    now = datetime(2026, 11, 1, tzinfo=UTC)
    raw = (
        spark.createDataFrame([{"id": 1}], SCHEMA)
        .withColumn("source_updated_at", F.lit(now))
        .withColumn("source_page", F.lit(1))
    )
    outputs = {
        "movies": to_movies(spark, raw, batch_id="demo", ingested_at=now),
        "movie_genres": to_genres(spark, raw, "demo"),
        "movie_countries": to_countries(spark, raw, "demo"),
    }
    for table, df in outputs.items():
        validate_table_schema(table, df.schema)


def target_columns(schema):
    sql_types = {
        LongType: "bigint",
        StringType: "text",
        TimestampType: "timestamp with time zone",
    }
    return [
        {
            "column_name": f.name,
            "data_type": sql_types[type(f.dataType)],
            "is_nullable": "YES",
        }
        for f in schema
    ]


def test_dynamic_schema_accepts_new_column():
    schema = StructType(
        TEST_SCHEMAS["movies"].fields + [StructField("new_field", StringType())]
    )
    value = manifest_with_schemas()
    value["tables"]["movies"]["schema"] = schema.jsonValue()
    validate_table_schema("movies", schema, manifest_table_schema(value, "movies"))
    validate_target_schema("movies", schema, target_columns(schema))
    assert "new_field" in manifest_table_columns(value, "movies")


@pytest.mark.parametrize(
    ("kind", "metadata", "accepted"),
    [
        (IntegerType(), {"data_type": "bigint"}, True),
        (LongType(), {"data_type": "integer"}, False),
        (LongType(), {"data_type": "double precision"}, False),
        (StringType(), {"data_type": "text"}, True),
        (
            StringType(),
            {"data_type": "character varying", "character_maximum_length": None},
            True,
        ),
        (
            StringType(),
            {"data_type": "character varying", "character_maximum_length": 10},
            False,
        ),
        (StringType(), {"data_type": "character varying"}, False),
        (BooleanType(), {"data_type": "boolean"}, True),
        (DateType(), {"data_type": "date"}, True),
        (TimestampType(), {"data_type": "timestamp with time zone"}, True),
        (TimestampType(), {"data_type": "timestamp without time zone"}, False),
        (DoubleType(), {"data_type": "real"}, False),
        (
            DecimalType(5, 2),
            {"data_type": "numeric", "numeric_precision": 8, "numeric_scale": 3},
            True,
        ),
        (
            DecimalType(5, 2),
            {"data_type": "numeric", "numeric_precision": 5, "numeric_scale": 1},
            False,
        ),
        (
            DecimalType(5, 2),
            {"data_type": "numeric", "numeric_precision": 4, "numeric_scale": 2},
            False,
        ),
        (
            LongType(),
            {"data_type": "numeric", "numeric_precision": 20, "numeric_scale": 2},
            False,
        ),
        (
            LongType(),
            {"data_type": "numeric", "numeric_precision": 21, "numeric_scale": 2},
            True,
        ),
        (
            DecimalType(5, 2),
            {"data_type": "numeric", "numeric_precision": None, "numeric_scale": None},
            True,
        ),
        (DecimalType(5, 2), {"data_type": "numeric"}, False),
        (ArrayType(StringType()), {"data_type": "text"}, False),
    ],
)
def test_target_type_compatibility(kind, metadata, accepted):
    schema = StructType(TEST_SCHEMAS["movies"].fields + [StructField("value", kind)])
    target = target_columns(TEST_SCHEMAS["movies"]) + [
        {"column_name": "value", "is_nullable": "YES", **metadata}
    ]
    if accepted:
        validate_target_schema("movies", schema, target)
    else:
        with pytest.raises(StagingValidationError, match="incompatible"):
            validate_target_schema("movies", schema, target)


def test_target_missing_and_extra_required_columns():
    schema = TEST_SCHEMAS["movies"]
    columns = target_columns(schema)
    with pytest.raises(StagingValidationError, match="target lacks"):
        validate_target_schema("movies", schema, columns[:-1])
    extra = {"column_name": "extra", "data_type": "text", "is_nullable": "NO"}
    with pytest.raises(StagingValidationError, match="missing required target"):
        validate_target_schema("movies", schema, columns + [extra])
    validate_target_schema(
        "movies", schema, columns + [{**extra, "column_default": "'default'"}]
    )
    validate_target_schema(
        "movies", schema, columns + [{**extra, "is_nullable": "YES"}]
    )


@pytest.mark.parametrize(
    "metadata", [[], [{"column_name": "id", "data_type": "bigint"}]]
)
def test_target_metadata_must_be_complete(metadata):
    with pytest.raises(StagingValidationError, match="metadata"):
        validate_target_schema("movies", TEST_SCHEMAS["movies"], metadata)


def test_target_nullability_checks_actual_rows(spark):
    metadata = {name: target_columns(schema) for name, schema in TEST_SCHEMAS.items()}
    for column in metadata["movies"]:
        if column["column_name"] == "name":
            column["is_nullable"] = "NO"
    with pytest.raises(StagingValidationError, match="NULL"):
        validate_tables(frames(spark), manifest_with_schemas(), metadata)
    assert (
        validate_tables(
            frames(spark, movie_overrides={"name": "Movie"}),
            manifest_with_schemas(),
            metadata,
        )["movies"]
        == 1
    )


def test_contract_contains_keys_targets_and_links():
    assert LOAD_CONTRACT["movies"]["target"] == "stg.movies"
    assert LOAD_CONTRACT["movie_genres"]["references"] == {"movies": ("batch_id", "id")}
