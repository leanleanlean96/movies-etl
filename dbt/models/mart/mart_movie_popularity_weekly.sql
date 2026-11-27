{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['week_start_date', 'movie_id'], partition_keys=['week_start_date'], on_schema_change='fail') }}

with weeks as (
    select distinct week_start_date, iso_year, iso_week_number,
        least(week_start_date + 7, '{{ var("snapshot_end_date", run_started_at.strftime("%Y-%m-%d")) }}'::date + 1)::timestamp at time zone 'UTC' as period_end
    from {{ ref('dim_date') }}
    where calendar_date <= '{{ var("snapshot_end_date", run_started_at.strftime("%Y-%m-%d")) }}'::date
      and calendar_date >= (select min((observed_at at time zone 'UTC')::date) from {{ ref('fact_movie_scores_history') }})
), movies as (select distinct movie_id from {{ ref('fact_movie_scores_history') }}), states as (
select w.week_start_date, w.iso_year, w.iso_week_number, m.movie_id,
    s.votes_kp as votes_start, e.votes_kp as votes_end,
    s.rating_kp as rating_start, e.rating_kp as rating_end,
    s.movie_id is not null as has_start_state, e.movie_id is not null as has_end_state,
    w.week_start_date + 7 <= '{{ var("snapshot_end_date", run_started_at.strftime("%Y-%m-%d")) }}'::date + 1 as is_complete_week,
    w.period_end
from weeks w cross join movies m
left join {{ ref('fact_movie_scores_history') }} s on s.movie_id = m.movie_id
    and s.valid_from < w.week_start_date::timestamp at time zone 'UTC'
    and (s.valid_to is null or s.valid_to >= w.week_start_date::timestamp at time zone 'UTC')
left join {{ ref('fact_movie_scores_history') }} e on e.movie_id = m.movie_id
    and e.valid_from < w.period_end
    and (e.valid_to is null or e.valid_to >= w.period_end)
)
select s.week_start_date, s.iso_year, s.iso_week_number, s.movie_id,
    s.votes_start, s.votes_end, s.rating_start, s.rating_end,
    s.has_start_state, s.has_end_state, s.is_complete_week,
    s.votes_end - s.votes_start as votes_delta, s.rating_end - s.rating_start as rating_delta,
    m.name, m.production_year, m.alternative_name, m.movie_length_minutes, m.age_rating, m.rating_mpaa
from states s
left join {{ ref('dim_movie') }} m on m.movie_id = s.movie_id
    and m.valid_from < s.period_end
    and (m.valid_to is null or m.valid_to >= s.period_end)
where s.has_start_state or s.has_end_state
