{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['movie_id', 'market_sk', 'valid_from'], partition_keys=['movie_id'], on_schema_change='fail') }}

{% set input_sql %}
select m.id as movie_id, k.market_sk, f.amount::numeric(20,2), c.currency_sk,
    m.source_updated_at, m.ingested_at as observed_at, m.batch_id
from {{ source('kinopoisk', 'movies') }} m
cross join lateral (values
    ('world', m.fees_world_value, m.fees_world_currency),
    ('russia', m.fees_russia_value, m.fees_russia_currency),
    ('usa', m.fees_usa_value, m.fees_usa_currency)
) f(market_code, amount, currency)
join {{ ref('dim_market') }} k on k.market_code = f.market_code
join {{ ref('dim_currency') }} c
on nullif(btrim(f.currency), '') is not distinct from c.source_currency_value
{% endset %}
{{ history_sql(input_sql, ['movie_id', 'market_sk'], ['amount', 'currency_sk']) }}
