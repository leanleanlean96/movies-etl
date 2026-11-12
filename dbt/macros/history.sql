{% macro history_sql(input_sql, keys, values) %}
with input as ({{ input_sql }}),
deduplicated as (
    select *, row_number() over (
        partition by {{ keys | join(', ') }}, source_updated_at
        order by observed_at, batch_id
    ) as observation_rank from input
), ordered as (
    select *, row_number() over w as state_rank,
        lag(jsonb_build_array({{ values | join(', ') }})) over w as previous_state
    from deduplicated where observation_rank = 1
    window w as (partition by {{ keys | join(', ') }} order by source_updated_at)
), changes as (
    select * from ordered where state_rank = 1
        or jsonb_build_array({{ values | join(', ') }}) is distinct from previous_state
)
select {{ keys | join(', ') }}, {{ values | join(', ') }},
    source_updated_at as valid_from,
    lead(source_updated_at) over (
        partition by {{ keys | join(', ') }} order by source_updated_at
    ) as valid_to,
    observed_at, batch_id
from changes
{% endmacro %}

{% macro movie_observations() %}
with genres as (
    select id, batch_id, array_agg(distinct genre_id order by genre_id) as genre_ids
    from {{ source('kinopoisk', 'movie_genres') }} group by id, batch_id
), countries as (
    select id, batch_id, array_agg(distinct country_id order by country_id) as country_ids
    from {{ source('kinopoisk', 'movie_countries') }} group by id, batch_id
), grouped as (
    select m.*, g.genre_ids as raw_genres, c.country_ids as raw_countries,
        sum(case when m.genres_complete is true or g.genre_ids is not null then 1 else 0 end) over w as genre_group,
        sum(case when m.countries_complete is true or c.country_ids is not null then 1 else 0 end) over w as country_group
    from {{ source('kinopoisk', 'movies') }} m
    left join genres g on g.id = m.id and g.batch_id = m.batch_id
    left join countries c on c.id = m.id and c.batch_id = m.batch_id
    window w as (partition by m.id order by m.source_updated_at, m.ingested_at, m.batch_id rows unbounded preceding)
)
select *,
    coalesce(first_value(raw_genres) over (partition by id, genre_group order by source_updated_at, ingested_at, batch_id), array[]::bigint[]) as genre_ids,
    coalesce(first_value(raw_countries) over (partition by id, country_group order by source_updated_at, ingested_at, batch_id), array[]::bigint[]) as country_ids
from grouped
{% endmacro %}
