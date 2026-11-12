{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['movie_sk'], partition_keys=['movie_id'], on_schema_change='fail') }}

with observations as ({{ movie_observations() }}), versions as (
{{ history_sql('select id as movie_id, name, alternative_name, year::smallint as production_year,\nmovie_length as movie_length_minutes, age_rating::smallint, rating_mpaa, genre_ids, country_ids,\nsource_updated_at, ingested_at as observed_at, batch_id from observations', ['movie_id'], ['name', 'alternative_name', 'production_year', 'movie_length_minutes', 'age_rating', 'rating_mpaa', 'genre_ids', 'country_ids'] ) }}
), external_ids as (
    select distinct on (id) id, imdb_id, tmdb_id, kp_hd_id
    from {{ source('kinopoisk', 'movies') }}
    order by id, source_updated_at desc, ingested_at desc, batch_id desc
)
select ('x' || substr(md5(v.movie_id::text || '|' ||
    to_char(v.valid_from at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')), 1, 15))::bit(60)::bigint as movie_sk,
    v.*, e.imdb_id, e.tmdb_id, e.kp_hd_id
from versions v join external_ids e on e.id = v.movie_id
