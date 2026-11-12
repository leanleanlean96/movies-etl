{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['genre_sk'], partition_keys=['genre_sk'], on_schema_change='fail') }}

with ranked as (
    select g.genre_id, g.genre_name, g.genre_slug, row_number() over (
        partition by g.genre_id
        order by m.source_updated_at desc, m.ingested_at desc, m.batch_id desc, g.genre_name desc nulls last
    ) as rn
    from {{ source('kinopoisk', 'movie_genres') }} g
    join {{ source('kinopoisk', 'movies') }} m on m.id = g.id and m.batch_id = g.batch_id
)
select genre_id::integer as genre_sk, genre_id, genre_name, genre_slug
from ranked where rn = 1
