{{ config(tags=['input_check']) }}
with currencies as (
    select budget_currency as currency from {{ source('kinopoisk', 'movies') }}
    union select fees_world_currency from {{ source('kinopoisk', 'movies') }}
    union select fees_russia_currency from {{ source('kinopoisk', 'movies') }}
    union select fees_usa_currency from {{ source('kinopoisk', 'movies') }}
)
select s.currency from currencies s left join {{ ref('currency_mapping') }} c
on nullif(btrim(s.currency), '') is not distinct from c.source_currency_value
where c.currency_sk is null
