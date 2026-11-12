{{ config(tags=['input_check']) }}
with observations as ({{ movie_observations() }}), states as (
    select distinct id, source_updated_at,
        name, alternative_name, year, movie_length, age_rating, rating_mpaa,
        imdb_id, tmdb_id, kp_hd_id, genre_ids, country_ids,
        budget_value, budget_currency,
        fees_world_value, fees_world_currency, fees_russia_value, fees_russia_currency,
        fees_usa_value, fees_usa_currency,
        rating_kp, rating_imdb, rating_tmdb, rating_film_critics, rating_russian_film_critics,
        votes_kp, votes_imdb, votes_tmdb, votes_film_critics, votes_russian_film_critics
    from observations
)
select id, source_updated_at from states group by id, source_updated_at having count(*) > 1
