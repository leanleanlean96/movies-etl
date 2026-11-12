from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

MONEY_SCHEMA = StructType(
    [
        StructField("value", LongType()),
        StructField("currency", StringType()),
    ]
)


SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField(
            "externalId",
            StructType(
                [
                    StructField("imdb", StringType()),
                    StructField("tmdb", LongType()),
                    StructField("kpHD", StringType()),
                ]
            ),
        ),
        StructField("name", StringType()),
        StructField("alternativeName", StringType()),
        StructField("type", StringType()),
        StructField("isSeries", BooleanType()),
        StructField("status", StringType()),
        StructField("year", IntegerType()),
        StructField("movieLength", IntegerType()),
        StructField("ageRating", IntegerType()),
        StructField("ratingMpaa", StringType()),
        StructField("ticketsOnSale", BooleanType()),
        StructField(
            "rating",
            StructType(
                [
                    StructField("kp", DecimalType(5, 2)),
                    StructField("imdb", DecimalType(5, 2)),
                    StructField("tmdb", DecimalType(5, 2)),
                    StructField("filmCritics", DecimalType(5, 2)),
                    StructField("russianFilmCritics", DecimalType(5, 2)),
                ]
            ),
        ),
        StructField(
            "votes",
            StructType(
                [
                    StructField("kp", LongType()),
                    StructField("imdb", LongType()),
                    StructField("tmdb", LongType()),
                    StructField("filmCritics", LongType()),
                    StructField("russianFilmCritics", LongType()),
                ]
            ),
        ),
        StructField("top10", IntegerType()),
        StructField("top250", IntegerType()),
        StructField("budget", MONEY_SCHEMA),
        StructField(
            "fees",
            StructType(
                [
                    StructField("world", MONEY_SCHEMA),
                    StructField("russia", MONEY_SCHEMA),
                    StructField("usa", MONEY_SCHEMA),
                ]
            ),
        ),
        StructField(
            "premiere",
            StructType(
                [
                    StructField("world", StringType()),
                    StructField("russia", StringType()),
                    StructField("digital", StringType()),
                    StructField("dvd", StringType()),
                    StructField("bluray", StringType()),
                ]
            ),
        ),
        StructField(
            "genres",
            ArrayType(
                StructType(
                    [
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                        StructField("slug", StringType()),
                    ]
                ),
                containsNull=True,
            ),
        ),
        StructField(
            "countries",
            ArrayType(
                StructType(
                    [
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                    ]
                ),
                containsNull=True,
            ),
        ),
        StructField("createdAt", StringType()),
        StructField("updatedAt", StringType()),
        StructField("_corrupt_record", StringType()),
    ]
)
