-- No refresh is allowed before build_dbt succeeds.

SET stop_refreshable_materialized_views_on_startup = 1;

CREATE DATABASE IF NOT EXISTS mart ENGINE = Atomic;

CREATE TABLE IF NOT EXISTS mart.mart_catalog_year_genre
AS ext_cloudberry.mart_catalog_year_genre
ENGINE = MergeTree ORDER BY (snapshot_date_sk, genre_sk);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.refresh_mart_catalog_year_genre
REFRESH AFTER 1 DAY SETTINGS refresh_retries = 0
TO mart.mart_catalog_year_genre
EMPTY AS
SELECT
    `snapshot_date_sk`,
    `snapshot_date`,
    `production_year`,
    `genre_sk`,
    `movies_total`
FROM ext_cloudberry.mart_catalog_year_genre
SETTINGS max_execution_time = 1200;

SYSTEM STOP VIEW mart.refresh_mart_catalog_year_genre;

CREATE TABLE IF NOT EXISTS mart.mart_catalog_year_country
AS ext_cloudberry.mart_catalog_year_country
ENGINE = MergeTree ORDER BY (snapshot_date_sk, country_sk);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.refresh_mart_catalog_year_country
REFRESH AFTER 1 DAY SETTINGS refresh_retries = 0
TO mart.mart_catalog_year_country
EMPTY AS
SELECT
    `snapshot_date_sk`,
    `snapshot_date`,
    `production_year`,
    `country_sk`,
    `movies_total`
FROM ext_cloudberry.mart_catalog_year_country
SETTINGS max_execution_time = 1200;

SYSTEM STOP VIEW mart.refresh_mart_catalog_year_country;

CREATE TABLE IF NOT EXISTS mart.mart_movie_scores_current
AS ext_cloudberry.mart_movie_scores_current
ENGINE = MergeTree ORDER BY (movie_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.refresh_mart_movie_scores_current
REFRESH AFTER 1 DAY SETTINGS refresh_retries = 0
TO mart.mart_movie_scores_current
EMPTY AS
SELECT
    `movie_id`,
    `rating_kp`,
    `votes_kp`,
    `rating_imdb`,
    `votes_imdb`,
    `rating_tmdb`,
    `votes_tmdb`,
    `rating_film_critics`,
    `votes_film_critics`,
    `rating_russian_film_critics`,
    `votes_russian_film_critics`,
    `valid_from`,
    `valid_to`,
    `observed_at`,
    `batch_id`,
    `name`,
    `production_year`
FROM ext_cloudberry.mart_movie_scores_current
SETTINGS max_execution_time = 1200;

SYSTEM STOP VIEW mart.refresh_mart_movie_scores_current;

CREATE TABLE IF NOT EXISTS mart.mart_budget_current_by_currency
AS ext_cloudberry.mart_budget_current_by_currency
ENGINE = MergeTree ORDER BY (currency_sk);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.refresh_mart_budget_current_by_currency
REFRESH AFTER 1 DAY SETTINGS refresh_retries = 0
TO mart.mart_budget_current_by_currency
EMPTY AS
SELECT
    `currency_sk`,
    `source_currency_value`,
    `currency_code`,
    `currency_name`,
    `movies_total`,
    `movies_with_budget`,
    `budget_total`,
    `budget_average`,
    `budget_coverage_pct`
FROM ext_cloudberry.mart_budget_current_by_currency
SETTINGS max_execution_time = 1200;

SYSTEM STOP VIEW mart.refresh_mart_budget_current_by_currency;

CREATE TABLE IF NOT EXISTS mart.mart_box_office_current_by_market_currency
AS ext_cloudberry.mart_box_office_current_by_market_currency
ENGINE = MergeTree ORDER BY (market_sk, currency_sk);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.refresh_mart_box_office_current_by_market_currency
REFRESH AFTER 1 DAY SETTINGS refresh_retries = 0
TO mart.mart_box_office_current_by_market_currency
EMPTY AS
SELECT
    `market_sk`,
    `currency_sk`,
    `source_currency_value`,
    `currency_code`,
    `currency_name`,
    `movies_total`,
    `movies_with_box_office`,
    `box_office_total`,
    `box_office_average`,
    `box_office_coverage_pct`
FROM ext_cloudberry.mart_box_office_current_by_market_currency
SETTINGS max_execution_time = 1200;

SYSTEM STOP VIEW mart.refresh_mart_box_office_current_by_market_currency;

CREATE TABLE IF NOT EXISTS mart.mart_budget_vs_box_office
AS ext_cloudberry.mart_budget_vs_box_office
ENGINE = MergeTree ORDER BY (movie_id, market_sk);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.refresh_mart_budget_vs_box_office
REFRESH AFTER 1 DAY SETTINGS refresh_retries = 0
TO mart.mart_budget_vs_box_office
EMPTY AS
SELECT
    `movie_id`,
    `market_sk`,
    `budget_amount`,
    `box_office_amount`,
    `budget_currency_sk`,
    `box_office_currency_sk`,
    `common_currency_code`,
    `box_office_to_budget_ratio`
FROM ext_cloudberry.mart_budget_vs_box_office
SETTINGS max_execution_time = 1200;

SYSTEM STOP VIEW mart.refresh_mart_budget_vs_box_office;

CREATE TABLE IF NOT EXISTS mart.mart_movie_popularity_weekly
AS ext_cloudberry.mart_movie_popularity_weekly
ENGINE = MergeTree ORDER BY (week_start_date, movie_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.refresh_mart_movie_popularity_weekly
REFRESH AFTER 1 DAY SETTINGS refresh_retries = 0
TO mart.mart_movie_popularity_weekly
EMPTY AS
SELECT
    `week_start_date`,
    `iso_year`,
    `iso_week_number`,
    `movie_id`,
    `votes_start`,
    `votes_end`,
    `rating_start`,
    `rating_end`,
    `has_start_state`,
    `has_end_state`,
    `is_complete_week`,
    `votes_delta`,
    `rating_delta`
FROM ext_cloudberry.mart_movie_popularity_weekly
SETTINGS max_execution_time = 1200;

SYSTEM STOP VIEW mart.refresh_mart_movie_popularity_weekly;

CREATE TABLE IF NOT EXISTS mart.mart_top250_daily_changes
AS ext_cloudberry.mart_top250_daily_changes
ENGINE = MergeTree ORDER BY (snapshot_date_sk, movie_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.refresh_mart_top250_daily_changes
REFRESH AFTER 1 DAY SETTINGS refresh_retries = 0
TO mart.mart_top250_daily_changes
EMPTY AS
SELECT
    `snapshot_date_sk`,
    `movie_id`,
    `previous_position`,
    `position`,
    `entered`,
    `exited`,
    `position_change`
FROM ext_cloudberry.mart_top250_daily_changes
SETTINGS max_execution_time = 1200;

SYSTEM STOP VIEW mart.refresh_mart_top250_daily_changes;
