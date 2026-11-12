select * from {{ ref('fact_movie_scores_history') }} where
rating_kp not between 0 and 10 or rating_imdb not between 0 and 10 or rating_tmdb not between 0 and 10
or rating_film_critics not between 0 and 100 or rating_russian_film_critics not between 0 and 100
or votes_kp < 0 or votes_imdb < 0 or votes_tmdb < 0 or votes_film_critics < 0 or votes_russian_film_critics < 0
