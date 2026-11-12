{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['snapshot_date_sk', 'position'], partition_keys=['snapshot_date_sk'], on_schema_change='fail') }}

with stats as materialized (
    select snapshot_date, batch_id, max(observed_at) as observed_at,
        count(*) as rows_count, count(distinct movie_id) as movies_count,
        count(distinct position) as positions_count,
        min(position) as first_position, max(position) as last_position
    from {{ source('kinopoisk', 'top250') }} group by snapshot_date, batch_id
), complete as (
    select * from stats where rows_count = 250 and movies_count = 250
        and positions_count = 250 and first_position = 1 and last_position = 250
), chosen as (
    select *, row_number() over (partition by snapshot_date order by observed_at desc, batch_id desc) as rn
    from complete
)
select to_char(t.snapshot_date, 'YYYYMMDD')::integer as snapshot_date_sk,
    t.position, t.movie_id, t.observed_at, t.batch_id
from {{ source('kinopoisk', 'top250') }} t join chosen c
on t.snapshot_date = c.snapshot_date and t.batch_id = c.batch_id and c.rn = 1
