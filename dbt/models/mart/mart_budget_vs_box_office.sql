{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['movie_id', 'market_sk'], partition_keys=['movie_id'], on_schema_change='fail') }}

select b.movie_id, f.market_sk, b.amount as budget_amount, f.amount as box_office_amount,
    b.currency_sk as budget_currency_sk, f.currency_sk as box_office_currency_sk,
    case when bc.currency_code = fc.currency_code then bc.currency_code end as common_currency_code,
    case when bc.currency_code = fc.currency_code and b.amount > 0
        then (f.amount / b.amount)::numeric(30,6) end as box_office_to_budget_ratio
from {{ ref('v_budget_current') }} b
join {{ ref('v_box_office_current') }} f on f.movie_id = b.movie_id
join {{ ref('dim_currency') }} bc on bc.currency_sk = b.currency_sk
join {{ ref('dim_currency') }} fc on fc.currency_sk = f.currency_sk
