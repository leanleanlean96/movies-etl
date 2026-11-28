-- Add columns in the order used by dbt's incremental publisher.
BEGIN;

ALTER TABLE IF EXISTS mart.mart_catalog_year_genre
    ADD COLUMN IF NOT EXISTS genre_name text,
    ADD COLUMN IF NOT EXISTS genre_slug text;

ALTER TABLE IF EXISTS mart.mart_catalog_year_country
    ADD COLUMN IF NOT EXISTS country_name text;

ALTER TABLE IF EXISTS mart.mart_movie_scores_current
    ADD COLUMN IF NOT EXISTS alternative_name text,
    ADD COLUMN IF NOT EXISTS movie_length_minutes integer,
    ADD COLUMN IF NOT EXISTS age_rating smallint,
    ADD COLUMN IF NOT EXISTS rating_mpaa text;

ALTER TABLE IF EXISTS mart.mart_box_office_current_by_market_currency
    ADD COLUMN IF NOT EXISTS market_code text,
    ADD COLUMN IF NOT EXISTS market_name text;

ALTER TABLE IF EXISTS mart.mart_budget_vs_box_office
    ADD COLUMN IF NOT EXISTS name text,
    ADD COLUMN IF NOT EXISTS production_year smallint,
    ADD COLUMN IF NOT EXISTS alternative_name text,
    ADD COLUMN IF NOT EXISTS movie_length_minutes integer,
    ADD COLUMN IF NOT EXISTS age_rating smallint,
    ADD COLUMN IF NOT EXISTS rating_mpaa text,
    ADD COLUMN IF NOT EXISTS market_code text,
    ADD COLUMN IF NOT EXISTS market_name text,
    ADD COLUMN IF NOT EXISTS budget_currency_code text,
    ADD COLUMN IF NOT EXISTS box_office_currency_code text;

ALTER TABLE IF EXISTS mart.mart_movie_popularity_weekly
    ADD COLUMN IF NOT EXISTS name text,
    ADD COLUMN IF NOT EXISTS production_year smallint,
    ADD COLUMN IF NOT EXISTS alternative_name text,
    ADD COLUMN IF NOT EXISTS movie_length_minutes integer,
    ADD COLUMN IF NOT EXISTS age_rating smallint,
    ADD COLUMN IF NOT EXISTS rating_mpaa text;

ALTER TABLE IF EXISTS mart.mart_top250_daily_changes
    ADD COLUMN IF NOT EXISTS name text,
    ADD COLUMN IF NOT EXISTS production_year smallint,
    ADD COLUMN IF NOT EXISTS alternative_name text,
    ADD COLUMN IF NOT EXISTS movie_length_minutes integer,
    ADD COLUMN IF NOT EXISTS age_rating smallint,
    ADD COLUMN IF NOT EXISTS rating_mpaa text;

COMMIT;
