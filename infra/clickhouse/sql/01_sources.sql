-- Source contracts are explicit; keep them in sync with dbt/models/mart.

CREATE DATABASE IF NOT EXISTS ext_cloudberry ENGINE = Atomic;

CREATE TABLE IF NOT EXISTS ext_cloudberry.mart_catalog_year_genre (
    `snapshot_date_sk` Int32,
    `snapshot_date` Nullable(Date32),
    `production_year` Nullable(Int16),
    `genre_sk` Int32,
    `movies_total` Nullable(Int64)
) ENGINE = PostgreSQL(cloudberry_marts, table = 'mart_catalog_year_genre');

CREATE TABLE IF NOT EXISTS ext_cloudberry.mart_catalog_year_country (
    `snapshot_date_sk` Int32,
    `snapshot_date` Nullable(Date32),
    `production_year` Nullable(Int16),
    `country_sk` Int32,
    `movies_total` Nullable(Int64)
) ENGINE = PostgreSQL(cloudberry_marts, table = 'mart_catalog_year_country');

CREATE TABLE IF NOT EXISTS ext_cloudberry.mart_movie_scores_current (
    `movie_id` Int64,
    `rating_kp` Nullable(Decimal(5, 2)),
    `votes_kp` Nullable(Int64),
    `rating_imdb` Nullable(Decimal(5, 2)),
    `votes_imdb` Nullable(Int64),
    `rating_tmdb` Nullable(Decimal(5, 2)),
    `votes_tmdb` Nullable(Int64),
    `rating_film_critics` Nullable(Decimal(5, 2)),
    `votes_film_critics` Nullable(Int64),
    `rating_russian_film_critics` Nullable(Decimal(5, 2)),
    `votes_russian_film_critics` Nullable(Int64),
    `valid_from` Nullable(DateTime64(6, 'UTC')),
    `valid_to` Nullable(DateTime64(6, 'UTC')),
    `observed_at` Nullable(DateTime64(6, 'UTC')),
    `batch_id` Nullable(String),
    `name` Nullable(String),
    `production_year` Nullable(Int16)
) ENGINE = PostgreSQL(cloudberry_marts, table = 'mart_movie_scores_current');

CREATE TABLE IF NOT EXISTS ext_cloudberry.mart_budget_current_by_currency (
    `currency_sk` Int32,
    `source_currency_value` Nullable(String),
    `currency_code` Nullable(String),
    `currency_name` Nullable(String),
    `movies_total` Nullable(Int64),
    `movies_with_budget` Nullable(Int64),
    `budget_total` Nullable(Decimal(30, 2)),
    `budget_average` Nullable(Decimal(30, 2)),
    `budget_coverage_pct` Nullable(Decimal(5, 2))
) ENGINE = PostgreSQL(cloudberry_marts, table = 'mart_budget_current_by_currency');

CREATE TABLE IF NOT EXISTS ext_cloudberry.mart_box_office_current_by_market_currency (
    `market_sk` Int32,
    `currency_sk` Int32,
    `source_currency_value` Nullable(String),
    `currency_code` Nullable(String),
    `currency_name` Nullable(String),
    `movies_total` Nullable(Int64),
    `movies_with_box_office` Nullable(Int64),
    `box_office_total` Nullable(Decimal(30, 2)),
    `box_office_average` Nullable(Decimal(30, 2)),
    `box_office_coverage_pct` Nullable(Decimal(5, 2))
) ENGINE = PostgreSQL(cloudberry_marts, table = 'mart_box_office_current_by_market_currency');

CREATE TABLE IF NOT EXISTS ext_cloudberry.mart_budget_vs_box_office (
    `movie_id` Int64,
    `market_sk` Int32,
    `budget_amount` Nullable(Decimal(20, 2)),
    `box_office_amount` Nullable(Decimal(20, 2)),
    `budget_currency_sk` Nullable(Int32),
    `box_office_currency_sk` Nullable(Int32),
    `common_currency_code` Nullable(String),
    `box_office_to_budget_ratio` Nullable(Decimal(30, 6))
) ENGINE = PostgreSQL(cloudberry_marts, table = 'mart_budget_vs_box_office');

CREATE TABLE IF NOT EXISTS ext_cloudberry.mart_movie_popularity_weekly (
    `week_start_date` Date32,
    `iso_year` Nullable(Int32),
    `iso_week_number` Nullable(Int32),
    `movie_id` Int64,
    `votes_start` Nullable(Int64),
    `votes_end` Nullable(Int64),
    `rating_start` Nullable(Decimal(5, 2)),
    `rating_end` Nullable(Decimal(5, 2)),
    `has_start_state` Nullable(Bool),
    `has_end_state` Nullable(Bool),
    `is_complete_week` Nullable(Bool),
    `votes_delta` Nullable(Int64),
    `rating_delta` Nullable(Decimal(6, 2))
) ENGINE = PostgreSQL(cloudberry_marts, table = 'mart_movie_popularity_weekly');

CREATE TABLE IF NOT EXISTS ext_cloudberry.mart_top250_daily_changes (
    `snapshot_date_sk` Int32,
    `movie_id` Int64,
    `previous_position` Nullable(Int32),
    `position` Nullable(Int32),
    `entered` Nullable(Bool),
    `exited` Nullable(Bool),
    `position_change` Nullable(Int32)
) ENGINE = PostgreSQL(cloudberry_marts, table = 'mart_top250_daily_changes');
