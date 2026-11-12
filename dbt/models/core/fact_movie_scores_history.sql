{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['movie_id', 'valid_from'], partition_keys=['movie_id'], on_schema_change='fail') }}

{% set input_sql %}
select id as movie_id, rating_kp, votes_kp, rating_imdb, votes_imdb, rating_tmdb, votes_tmdb, rating_film_critics, votes_film_critics, rating_russian_film_critics, votes_russian_film_critics , source_updated_at, ingested_at as observed_at, batch_id
from {{ source('kinopoisk', 'movies') }}
{% endset %}
{{ history_sql(input_sql, ['movie_id'], ['rating_kp', 'votes_kp', 'rating_imdb', 'votes_imdb', 'rating_tmdb', 'votes_tmdb', 'rating_film_critics', 'votes_film_critics', 'rating_russian_film_critics', 'votes_russian_film_critics']) }}
