{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['date_sk'], partition_keys=['date_sk'], on_schema_change='fail') }}

with bounds as (
    select coalesce(min((ingested_at at time zone 'UTC')::date), {{ dbt.string_literal(var('snapshot_end_date', run_started_at.strftime('%Y-%m-%d'))) }}::date) as first_date
    from {{ source('kinopoisk', 'movies') }}
), days as (
    select generate_series(least(first_date, '{{ var("calendar_start_date", "2000-01-01") }}'::date)::timestamp,
        greatest('{{ var("calendar_end_date", "2035-12-31") }}'::date,
        '{{ var("snapshot_end_date", run_started_at.strftime("%Y-%m-%d")) }}'::date)::timestamp,
        interval '1 day')::date as d from bounds
)
select to_char(d, 'YYYYMMDD')::integer as date_sk, d as calendar_date,
    extract(year from d)::integer as calendar_year, extract(quarter from d)::integer as quarter_number,
    extract(month from d)::integer as month_number, extract(day from d)::integer as day_of_month,
    extract(isodow from d)::integer as iso_day_of_week, extract(week from d)::integer as iso_week_number,
    extract(isoyear from d)::integer as iso_year, date_trunc('week', d)::date as week_start_date,
    date_trunc('month', d)::date as month_start_date, extract(isodow from d) in (6,7) as is_weekend
from days
