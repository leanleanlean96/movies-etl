{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['snapshot_date_sk', 'movie_id'], partition_keys=['snapshot_date_sk'], on_schema_change='fail') }}

with dates as (
    select distinct snapshot_date_sk, to_date(snapshot_date_sk::text, 'YYYYMMDD') as snapshot_date
    from {{ ref('fact_top250_daily') }}
), pairs as (
    select c.snapshot_date_sk, p.snapshot_date_sk as previous_date_sk
    from dates c join dates p on p.snapshot_date = c.snapshot_date - 1
), members as (
    select p.snapshot_date_sk, p.previous_date_sk, t.movie_id
    from pairs p join {{ ref('fact_top250_daily') }} t
    on t.snapshot_date_sk in (p.snapshot_date_sk, p.previous_date_sk)
    group by p.snapshot_date_sk, p.previous_date_sk, t.movie_id
)
select m.snapshot_date_sk, m.movie_id, p.position as previous_position, c.position,
    p.position is null as entered, c.position is null as exited,
    p.position - c.position as position_change
from members m
left join {{ ref('fact_top250_daily') }} c on c.snapshot_date_sk = m.snapshot_date_sk and c.movie_id = m.movie_id
left join {{ ref('fact_top250_daily') }} p on p.snapshot_date_sk = m.previous_date_sk and p.movie_id = m.movie_id
