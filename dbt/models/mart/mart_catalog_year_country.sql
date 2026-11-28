{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['snapshot_date_sk', 'production_year', 'country_sk'], partition_keys=['snapshot_date_sk'], on_schema_change='fail') }}

select f.snapshot_date_sk, d.calendar_date as snapshot_date,
    m.production_year, coalesce(b.country_sk, 0) as country_sk,
    count(distinct f.movie_id) as movies_total, c.country_name
from {{ ref('fact_movie_catalog_daily') }} f
join {{ ref('dim_date') }} d on d.date_sk = f.snapshot_date_sk
join {{ ref('dim_movie') }} m on m.movie_id = f.movie_id
    and m.valid_from < (d.calendar_date + 1)::timestamp at time zone 'UTC'
    and (m.valid_to is null or m.valid_to >= (d.calendar_date + 1)::timestamp at time zone 'UTC')
left join {{ ref('bridge_movie_country') }} b on b.movie_sk = m.movie_sk
left join {{ ref('dim_country') }} c on c.country_sk = b.country_sk
group by f.snapshot_date_sk, d.calendar_date, m.production_year, coalesce(b.country_sk, 0),
    c.country_name
