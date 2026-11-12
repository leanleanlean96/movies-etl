CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.movies (
    id                          bigint      NOT NULL,
    batch_id                    text        NOT NULL,
    source_page                 integer,
    source_updated_at           timestamptz NOT NULL,
    created_at                  timestamptz,
    ingested_at                 timestamptz NOT NULL,
    genres_complete             boolean,
    countries_complete          boolean,

    type                        text,
    name                        text,
    alternative_name            text,
    year                        integer,
    movie_length                integer,
    age_rating                  integer,
    rating_mpaa                 text,
    status                      text,
    is_series                   boolean,
    tickets_on_sale             boolean,

    imdb_id                     text,
    tmdb_id                     bigint,
    kp_hd_id                    text,

    rating_kp                   numeric(5,2),
    rating_imdb                 numeric(5,2),
    rating_tmdb                 numeric(5,2),
    rating_film_critics          numeric(5,2),
    rating_russian_film_critics  numeric(5,2),

    votes_kp                    bigint,
    votes_imdb                  bigint,
    votes_tmdb                  bigint,
    votes_film_critics           bigint,
    votes_russian_film_critics   bigint,

    top10                       integer,
    top250                      integer,

    budget_value                bigint,
    budget_currency             text,
    fees_world_value            bigint,
    fees_world_currency         text,
    fees_russia_value           bigint,
    fees_russia_currency        text,
    fees_usa_value              bigint,
    fees_usa_currency           text,

    premiere_world              date,
    premiere_russia             date,
    premiere_digital            date,
    premiere_dvd                date,
    premiere_bluray             date,

    CONSTRAINT movies_pk PRIMARY KEY (batch_id, id)
)
DISTRIBUTED BY (id);

CREATE TABLE IF NOT EXISTS stg.movie_genres (
    id          bigint NOT NULL,
    genre_id    bigint NOT NULL,
    genre_name  text,
    genre_slug  text,
    batch_id    text   NOT NULL,

    CONSTRAINT movie_genres_pk PRIMARY KEY (batch_id, id, genre_id)
)
DISTRIBUTED BY (id);

CREATE TABLE IF NOT EXISTS stg.movie_countries (
    id            bigint NOT NULL,
    country_id    bigint NOT NULL,
    country_name  text,
    batch_id      text   NOT NULL,

    CONSTRAINT movie_countries_pk PRIMARY KEY (batch_id, id, country_id)
)
DISTRIBUTED BY (id);
