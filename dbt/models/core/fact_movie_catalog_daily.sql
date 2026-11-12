{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['snapshot_date_sk', 'movie_id'], partition_keys=['snapshot_date_sk'], on_schema_change='fail') }}

with first_seen as (
    select distinct on (id) id as movie_id, ingested_at, batch_id
    from {{ source('kinopoisk', 'movies') }} order by id, ingested_at, batch_id
)
select d.date_sk as snapshot_date_sk, f.movie_id,
    (d.calendar_date + 1)::timestamp at time zone 'UTC' as built_at, f.batch_id
from first_seen f join {{ ref('dim_date') }} d
on d.calendar_date >= (f.ingested_at at time zone 'UTC')::date
where d.calendar_date <= '{{ var("snapshot_end_date", run_started_at.strftime("%Y-%m-%d")) }}'::date
