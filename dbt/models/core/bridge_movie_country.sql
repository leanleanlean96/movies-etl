{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['movie_sk', 'country_sk'], partition_keys=['movie_sk'], on_schema_change='fail') }}

select m.movie_sk, d.country_sk
from {{ ref('dim_movie') }} m
cross join lateral unnest(m.country_ids) as ids(source_id)
join {{ ref('dim_country') }} d on d.country_id = ids.source_id
