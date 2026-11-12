{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['week_start_date', 'movie_id'], partition_keys=['week_start_date'], on_schema_change='fail') }}

with weeks as (
    select distinct week_start_date, iso_year, iso_week_number
    from {{ ref('dim_date') }}
    where calendar_date <= '{{ var("snapshot_end_date", run_started_at.strftime("%Y-%m-%d")) }}'::date
      and calendar_date >= (select min((observed_at at time zone 'UTC')::date) from {{ ref('fact_movie_scores_history') }})
), movies as (select distinct movie_id from {{ ref('fact_movie_scores_history') }}), states as (
select w.*, m.movie_id, s.votes_kp as votes_start, e.votes_kp as votes_end,
    s.rating_kp as rating_start, e.rating_kp as rating_end,
    s.movie_id is not null as has_start_state, e.movie_id is not null as has_end_state,
    w.week_start_date + 7 <= '{{ var("snapshot_end_date", run_started_at.strftime("%Y-%m-%d")) }}'::date + 1 as is_complete_week
from weeks w cross join movies m
left join {{ ref('fact_movie_scores_history') }} s on s.movie_id = m.movie_id
    and s.valid_from < w.week_start_date::timestamp at time zone 'UTC'
    and (s.valid_to is null or s.valid_to >= w.week_start_date::timestamp at time zone 'UTC')
left join {{ ref('fact_movie_scores_history') }} e on e.movie_id = m.movie_id
    and e.valid_from < least(w.week_start_date + 7, '{{ var("snapshot_end_date", run_started_at.strftime("%Y-%m-%d")) }}'::date + 1)::timestamp at time zone 'UTC'
    and (e.valid_to is null or e.valid_to >= least(w.week_start_date + 7, '{{ var("snapshot_end_date", run_started_at.strftime("%Y-%m-%d")) }}'::date + 1)::timestamp at time zone 'UTC')
)
select *, votes_end - votes_start as votes_delta, rating_end - rating_start as rating_delta
from states where has_start_state or has_end_state
