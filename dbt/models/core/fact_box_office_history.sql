{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['movie_id', 'market_sk', 'valid_from'], partition_keys=['movie_id'], on_schema_change='fail') }}

{% set input_sql %}
with fees as (
    {% for market in ['world', 'russia', 'usa'] %}
    select id as movie_id, '{{ market }}' as market_code,
        fees_{{ market }}_value as amount, fees_{{ market }}_currency as currency,
        source_updated_at, ingested_at as observed_at, batch_id
    from {{ source('kinopoisk', 'movies') }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
select f.movie_id, k.market_sk, f.amount::numeric(20,2), c.currency_sk,
    f.source_updated_at, f.observed_at, f.batch_id
from fees f
join {{ ref('dim_market') }} k on k.market_code = f.market_code
join {{ ref('dim_currency') }} c
on nullif(btrim(f.currency), '') is not distinct from c.source_currency_value
{% endset %}
{{ history_sql(input_sql, ['movie_id', 'market_sk'], ['amount', 'currency_sk']) }}
