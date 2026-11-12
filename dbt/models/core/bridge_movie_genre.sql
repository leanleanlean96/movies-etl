{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['movie_sk', 'genre_sk'], partition_keys=['movie_sk'], on_schema_change='fail') }}

select m.movie_sk, d.genre_sk
from {{ ref('dim_movie') }} m
cross join lateral unnest(m.genre_ids) as ids(source_id)
join {{ ref('dim_genre') }} d on d.genre_id = ids.source_id
