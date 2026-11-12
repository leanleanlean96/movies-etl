import re

MANIFEST_VERSION = 2


CHECKSUM_ALGORITHM = "sha256"


SHA256_RE = re.compile(r"[0-9a-f]{64}")


TABLE_NAME_RE = re.compile(r"[a-z][a-z0-9_]*")


CLEANED_MANIFEST_VERSION = 1


CLEANED_SOURCE = "kinopoisk"


DEFAULT_OUTPUT_BUCKET = "etl-cleaned"


TOTAL_PARTS = ("movies", "quarantined", "duplicates")


QUARANTINE_REASONS = (
    "malformed_json",
    "missing_id",
    "bad_updated_at",
    "outside_interval",
    "unexpected_type",
)


TABLE_STATS = {
    "movies": "movies",
    "movie_genres": "genre_links",
    "movie_countries": "country_links",
    "quarantine": "quarantined",
}


TABLE_SORT_KEYS = {
    "movies": ["id"],
    "movie_genres": ["id", "genre_id"],
    "movie_countries": ["id", "country_id"],
    "quarantine": ["source_page", "reason", "id"],
}
