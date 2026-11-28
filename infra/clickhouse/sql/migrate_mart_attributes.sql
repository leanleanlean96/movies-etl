-- Run with publication paused, before applying 01_sources.sql and 02_views.sql.
-- The local tables retain their previously published rows until refresh.

ALTER TABLE mart.mart_catalog_year_genre
    ADD COLUMN IF NOT EXISTS `genre_name` Nullable(String),
    ADD COLUMN IF NOT EXISTS `genre_slug` Nullable(String);

ALTER TABLE mart.mart_catalog_year_country
    ADD COLUMN IF NOT EXISTS `country_name` Nullable(String);

ALTER TABLE mart.mart_movie_scores_current
    ADD COLUMN IF NOT EXISTS `alternative_name` Nullable(String),
    ADD COLUMN IF NOT EXISTS `movie_length_minutes` Nullable(Int32),
    ADD COLUMN IF NOT EXISTS `age_rating` Nullable(Int16),
    ADD COLUMN IF NOT EXISTS `rating_mpaa` Nullable(String);

ALTER TABLE mart.mart_box_office_current_by_market_currency
    ADD COLUMN IF NOT EXISTS `market_code` Nullable(String),
    ADD COLUMN IF NOT EXISTS `market_name` Nullable(String);

ALTER TABLE mart.mart_budget_vs_box_office
    ADD COLUMN IF NOT EXISTS `name` Nullable(String),
    ADD COLUMN IF NOT EXISTS `production_year` Nullable(Int16),
    ADD COLUMN IF NOT EXISTS `alternative_name` Nullable(String),
    ADD COLUMN IF NOT EXISTS `movie_length_minutes` Nullable(Int32),
    ADD COLUMN IF NOT EXISTS `age_rating` Nullable(Int16),
    ADD COLUMN IF NOT EXISTS `rating_mpaa` Nullable(String),
    ADD COLUMN IF NOT EXISTS `market_code` Nullable(String),
    ADD COLUMN IF NOT EXISTS `market_name` Nullable(String),
    ADD COLUMN IF NOT EXISTS `budget_currency_code` Nullable(String),
    ADD COLUMN IF NOT EXISTS `box_office_currency_code` Nullable(String);

ALTER TABLE mart.mart_movie_popularity_weekly
    ADD COLUMN IF NOT EXISTS `name` Nullable(String),
    ADD COLUMN IF NOT EXISTS `production_year` Nullable(Int16),
    ADD COLUMN IF NOT EXISTS `alternative_name` Nullable(String),
    ADD COLUMN IF NOT EXISTS `movie_length_minutes` Nullable(Int32),
    ADD COLUMN IF NOT EXISTS `age_rating` Nullable(Int16),
    ADD COLUMN IF NOT EXISTS `rating_mpaa` Nullable(String);

ALTER TABLE mart.mart_top250_daily_changes
    ADD COLUMN IF NOT EXISTS `name` Nullable(String),
    ADD COLUMN IF NOT EXISTS `production_year` Nullable(Int16),
    ADD COLUMN IF NOT EXISTS `alternative_name` Nullable(String),
    ADD COLUMN IF NOT EXISTS `movie_length_minutes` Nullable(Int32),
    ADD COLUMN IF NOT EXISTS `age_rating` Nullable(Int16),
    ADD COLUMN IF NOT EXISTS `rating_mpaa` Nullable(String);

DROP VIEW IF EXISTS mart.refresh_mart_catalog_year_genre SYNC;
DROP TABLE IF EXISTS ext_cloudberry.mart_catalog_year_genre SYNC;
DROP VIEW IF EXISTS mart.refresh_mart_catalog_year_country SYNC;
DROP TABLE IF EXISTS ext_cloudberry.mart_catalog_year_country SYNC;
DROP VIEW IF EXISTS mart.refresh_mart_movie_scores_current SYNC;
DROP TABLE IF EXISTS ext_cloudberry.mart_movie_scores_current SYNC;
DROP VIEW IF EXISTS mart.refresh_mart_box_office_current_by_market_currency SYNC;
DROP TABLE IF EXISTS ext_cloudberry.mart_box_office_current_by_market_currency SYNC;
DROP VIEW IF EXISTS mart.refresh_mart_budget_vs_box_office SYNC;
DROP TABLE IF EXISTS ext_cloudberry.mart_budget_vs_box_office SYNC;
DROP VIEW IF EXISTS mart.refresh_mart_movie_popularity_weekly SYNC;
DROP TABLE IF EXISTS ext_cloudberry.mart_movie_popularity_weekly SYNC;
DROP VIEW IF EXISTS mart.refresh_mart_top250_daily_changes SYNC;
DROP TABLE IF EXISTS ext_cloudberry.mart_top250_daily_changes SYNC;
