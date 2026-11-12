{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['market_sk', 'currency_sk'], partition_keys=['market_sk', 'currency_sk'], on_schema_change='fail') }}

select f.market_sk, c.currency_sk, c.source_currency_value, c.currency_code, c.currency_name,
count(*) as movies_total, count(f.amount) as movies_with_box_office,
case when c.currency_code is not null then sum(f.amount)::numeric(30,2) end as box_office_total,
case when c.currency_code is not null then avg(f.amount)::numeric(30,2) end as box_office_average,
round(100.0 * count(f.amount) / nullif(count(*), 0), 2)::numeric(5,2) as box_office_coverage_pct
from {{ ref('v_box_office_current') }} f join {{ ref('dim_currency') }} c on c.currency_sk = f.currency_sk
group by f.market_sk, c.currency_sk, c.source_currency_value, c.currency_code, c.currency_name
