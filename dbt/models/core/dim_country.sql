{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['country_sk'], partition_keys=['country_sk'], on_schema_change='fail') }}

with ranked as (
    select g.country_id, g.country_name, row_number() over (
        partition by g.country_id
        order by m.source_updated_at desc, m.ingested_at desc, m.batch_id desc, g.country_name desc nulls last
    ) as rn
    from {{ source('kinopoisk', 'movie_countries') }} g
    join {{ source('kinopoisk', 'movies') }} m on m.id = g.id and m.batch_id = g.batch_id
)
select country_id::integer as country_sk, country_id, country_name
from ranked where rn = 1
