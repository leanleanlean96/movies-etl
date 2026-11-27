{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['snapshot_date_sk', 'movie_id'], partition_keys=['snapshot_date_sk'], on_schema_change='fail') }}

with dates as (
    select distinct snapshot_date_sk, to_date(snapshot_date_sk::text, 'YYYYMMDD') as snapshot_date
    from {{ ref('fact_top250_daily') }}
), pairs as (
    select c.snapshot_date_sk, p.snapshot_date_sk as previous_date_sk,
        (c.snapshot_date + 1)::timestamp at time zone 'UTC' as day_end
    from dates c join dates p on p.snapshot_date = c.snapshot_date - 1
), members as (
    select p.snapshot_date_sk, p.previous_date_sk, t.movie_id, p.day_end
    from pairs p join {{ ref('fact_top250_daily') }} t
    on t.snapshot_date_sk in (p.snapshot_date_sk, p.previous_date_sk)
    group by p.snapshot_date_sk, p.previous_date_sk, t.movie_id, p.day_end
)
select m.snapshot_date_sk, m.movie_id, p.position as previous_position, c.position,
    p.position is null as entered, c.position is null as exited,
    p.position - c.position as position_change,
    d.name, d.production_year, d.alternative_name, d.movie_length_minutes, d.age_rating, d.rating_mpaa
from members m
left join {{ ref('fact_top250_daily') }} c on c.snapshot_date_sk = m.snapshot_date_sk and c.movie_id = m.movie_id
left join {{ ref('fact_top250_daily') }} p on p.snapshot_date_sk = m.previous_date_sk and p.movie_id = m.movie_id
left join {{ ref('dim_movie') }} d on d.movie_id = m.movie_id
    and d.valid_from < m.day_end
    and (d.valid_to is null or d.valid_to >= m.day_end)
