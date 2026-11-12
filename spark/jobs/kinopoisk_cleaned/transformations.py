from collections.abc import Mapping
from datetime import datetime
from typing import Any
from urllib.parse import unquote

from pyspark.sql import SparkSession, Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    broadcast,
    col,
    exists,
    explode,
    lit,
    nullif,
    row_number,
    struct,
    to_json,
    trim,
    try_to_date,
    try_to_timestamp,
    when,
)
from pyspark.sql.types import StructType, TimestampType

from .exceptions import PageIntegrityError
from .paths import page_numbers


def read_pages(
    spark: SparkSession,
    s3_uris: list[str],
    schema: StructType,
    *,
    manifest: Mapping[str, Any],
) -> DataFrame:
    df = (
        spark.read.schema(schema)
        .option("multiLine", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(s3_uris)
        .select("*", col("_metadata.file_path").alias("_source_path"))
    )

    numbers = page_numbers(manifest)
    mapping_rows = []
    for row in df.select("_source_path").distinct().collect():
        path = row["_source_path"]
        decoded_path = unquote(path)
        matches = [
            (key, number)
            for key, number in numbers.items()
            if decoded_path.endswith(f"/{key}")
        ]
        if len(matches) != 1:
            raise PageIntegrityError(
                f"{path}: expected exactly one matching manifest page, "
                f"found {len(matches)}"
            )
        key, number = matches[0]
        mapping_rows.append((path, key, number))

    mapping = spark.createDataFrame(
        mapping_rows,
        "_source_path string, source_key string, source_page int",
    )
    return (
        df.join(broadcast(mapping), "_source_path", "left").drop("_source_path").cache()
    )


def classify(
    spark: SparkSession,
    df: DataFrame,
    expected_type: str | None,
    interval_start: datetime,
    interval_end: datetime,
) -> DataFrame:
    unexpected_type = col("type").isNull()
    if expected_type is not None:
        unexpected_type = unexpected_type | ~col("type").eqNullSafe(lit(expected_type))

    df = df.withColumn("source_updated_at", col("updatedAt").try_cast(TimestampType()))
    df_classified = df.withColumn(
        "reason",
        when(col("_corrupt_record").isNotNull(), "malformed_json")
        .when((col("id") <= 0) | col("id").isNull(), "missing_id")
        .when(col("source_updated_at").isNull(), "bad_updated_at")
        .when(
            ~(
                (interval_start <= col("source_updated_at"))
                & (col("source_updated_at") < interval_end)
            ),
            "outside_interval",
        )
        .when(unexpected_type, "unexpected_type")
        .otherwise(lit(None)),
    )

    return df_classified


def quarantine(spark: SparkSession, df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.select(
            "id",
            "source_key",
            "source_page",
            "type",
            "reason",
            col("updatedAt").alias("updated_at_raw"),
            col("_corrupt_record").alias("corrupt_record"),
        )
        .withColumn("batch_id", lit(batch_id).alias("batch_id"))
        .where(col("reason").isNotNull())
    )


def deduplicate(spark: SparkSession, df: DataFrame) -> tuple[DataFrame, int]:
    ranking_window = Window.partitionBy("id").orderBy(
        col("source_updated_at").desc(),
        col("source_page").desc(),
        to_json(struct(*[col(name) for name in sorted(df.columns)])).desc(),
    )
    df_ranked = df.withColumn("rnk", row_number().over(ranking_window))
    dups_count = df_ranked.where(col("rnk") > 1).count()
    df_unduped = df_ranked.where(col("rnk") == 1).drop(col("rnk"))
    return df_unduped, dups_count


def to_movies(
    spark: SparkSession,
    df: DataFrame,
    *,
    batch_id: str,
    ingested_at: datetime | str,
) -> DataFrame:
    df_flattened = df.select(
        col("id"),
        lit(batch_id).alias("batch_id"),
        col("source_page"),
        col("source_updated_at"),
        # Preserve explicit empty sets before explode drops them from link tables.
        # Invalid links are filtered below and must not imply a complete empty set.
        *[
            (
                col(field).isNotNull()
                & ~exists(col(field), lambda item: item["id"].isNull())
            ).alias(f"{field}_complete")
            for field in ("genres", "countries")
        ],
        col("createdAt").alias("created_at"),
        lit(ingested_at).alias("ingested_at"),
        col("type"),
        col("name"),
        col("alternativeName").alias("alternative_name"),
        col("year"),
        col("movieLength").alias("movie_length"),
        col("ageRating").alias("age_rating"),
        col("ratingMpaa").alias("rating_mpaa"),
        col("status"),
        col("isSeries").alias("is_series"),
        col("ticketsOnSale").alias("tickets_on_sale"),
        col("externalId.imdb").alias("imdb_id"),
        col("externalId.tmdb").alias("tmdb_id"),
        col("externalId.kpHD").alias("kp_hd_id"),
        col("rating.kp").alias("rating_kp"),
        col("rating.imdb").alias("rating_imdb"),
        col("rating.filmCritics").alias("rating_film_critics"),
        col("rating.tmdb").alias("rating_tmdb"),
        col("rating.russianFilmCritics").alias("rating_russian_film_critics"),
        col("votes.kp").alias("votes_kp"),
        col("votes.imdb").alias("votes_imdb"),
        col("votes.filmCritics").alias("votes_film_critics"),
        col("votes.russianFilmCritics").alias("votes_russian_film_critics"),
        col("votes.tmdb").alias("votes_tmdb"),
        col("top10"),
        col("top250"),
        col("budget.value").alias("budget_value"),
        col("budget.currency").alias("budget_currency"),
        col("fees.world.value").alias("fees_world_value"),
        col("fees.world.currency").alias("fees_world_currency"),
        col("fees.russia.value").alias("fees_russia_value"),
        col("fees.russia.currency").alias("fees_russia_currency"),
        col("fees.usa.value").alias("fees_usa_value"),
        col("fees.usa.currency").alias("fees_usa_currency"),
        col("premiere.world").alias("premiere_world"),
        col("premiere.russia").alias("premiere_russia"),
        col("premiere.digital").alias("premiere_digital"),
        col("premiere.dvd").alias("premiere_dvd"),
        col("premiere.bluray").alias("premiere_bluray"),
    )

    df_casted = (
        df_flattened.withColumn("created_at", try_to_timestamp(col("created_at")))
        .withColumn("ingested_at", try_to_timestamp(col("ingested_at")))
        .withColumn("premiere_world", try_to_date(col("premiere_world")))
        .withColumn("premiere_russia", try_to_date(col("premiere_russia")))
        .withColumn("premiere_digital", try_to_date(col("premiere_digital")))
        .withColumn("premiere_dvd", try_to_date(col("premiere_dvd")))
        .withColumn("premiere_bluray", try_to_date(col("premiere_bluray")))
        .withColumn("rating_kp", col("rating_kp").try_cast("decimal(5, 2)"))
        .withColumn("rating_imdb", col("rating_imdb").try_cast("decimal(5, 2)"))
        .withColumn("rating_tmdb", col("rating_tmdb").try_cast("decimal(5, 2)"))
        .withColumn(
            "rating_film_critics", col("rating_film_critics").try_cast("decimal(5, 2)")
        )
        .withColumn(
            "rating_russian_film_critics",
            col("rating_russian_film_critics").try_cast("decimal(5, 2)"),
        )
    )

    for column, data_type in df_casted.dtypes:
        if data_type == "string" and column != "batch_id":
            df_casted = df_casted.withColumn(
                f"{column}", nullif(trim(col(f"{column}")), lit(""))
            )

    for column in ("budget", "fees_world", "fees_russia", "fees_usa"):
        value_column = f"{column}_value"
        currency_column = f"{column}_currency"
        df_casted = df_casted.withColumn(
            currency_column,
            when(col(value_column).isNotNull(), col(currency_column)).otherwise(
                lit(None)
            ),
        )

    return df_casted


def to_genres(spark: SparkSession, df: DataFrame, batch_id: str) -> DataFrame:
    df = (
        df.select("id", "genres")
        .withColumn("batch_id", lit(batch_id))
        .withColumn("genre", explode(col("genres")))
    )

    df = (
        df.withColumn("genre_id", col("genre.id"))
        .withColumn("genre_name", nullif(trim(col("genre.name")), lit("")))
        .withColumn("genre_slug", nullif(trim(col("genre.slug")), lit("")))
    )

    genres = df.select("id", "genre_id", "genre_name", "genre_slug", "batch_id").where(
        col("genre_id").isNotNull()
    )

    genre_window = Window.partitionBy("id", "genre_id").orderBy(
        col("genre_name").asc_nulls_last(), col("genre_slug").asc_nulls_last()
    )
    return (
        genres.withColumn("_genre_rank", row_number().over(genre_window))
        .where(col("_genre_rank") == 1)
        .drop("_genre_rank")
    )


def to_countries(spark: SparkSession, df: DataFrame, batch_id: str) -> DataFrame:
    countries = (
        df.select("id", explode(col("countries")).alias("country"))
        .where(col("country.id").isNotNull())
        .select(
            col("id"),
            col("country.id").alias("country_id"),
            nullif(trim(col("country.name")), lit("")).alias("country_name"),
            lit(batch_id).alias("batch_id"),
        )
    )

    country_window = Window.partitionBy("id", "country_id").orderBy(
        col("country_name").asc_nulls_last()
    )
    return (
        countries.withColumn("_country_rank", row_number().over(country_window))
        .where(col("_country_rank") == 1)
        .drop("_country_rank")
    )
