{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['movie_id', 'valid_from'], partition_keys=['movie_id'], on_schema_change='fail') }}

{% set input_sql %}
select m.id as movie_id, m.budget_value::numeric(20,2) as amount, c.currency_sk,
    m.source_updated_at, m.ingested_at as observed_at, m.batch_id
from {{ source('kinopoisk', 'movies') }} m
join {{ ref('dim_currency') }} c
on nullif(btrim(m.budget_currency), '') is not distinct from c.source_currency_value
{% endset %}
{{ history_sql(input_sql, ['movie_id'], ['amount', 'currency_sk']) }}
