{{ config(tags=['input_check']) }}
select id, batch_id from {{ source('kinopoisk', 'movies') }}
where id is null or batch_id is null or source_updated_at is null or ingested_at is null
    or year not between -32768 and 32767 or age_rating not between -32768 and 32767
union all
select id, batch_id from {{ source('kinopoisk', 'movies') }} group by id, batch_id having count(*) > 1
union all
select g.id, g.batch_id from {{ source('kinopoisk', 'movie_genres') }} g
left join {{ source('kinopoisk', 'movies') }} m on m.id = g.id and m.batch_id = g.batch_id
where m.id is null or g.genre_id is null or g.genre_id <= 0 or g.genre_id > 2147483647
union all
select g.id, g.batch_id from {{ source('kinopoisk', 'movie_countries') }} g
left join {{ source('kinopoisk', 'movies') }} m on m.id = g.id and m.batch_id = g.batch_id
where m.id is null or g.country_id is null or g.country_id <= 0 or g.country_id > 2147483647
