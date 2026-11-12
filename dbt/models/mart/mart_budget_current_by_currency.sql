{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['currency_sk'], partition_keys=['currency_sk'], on_schema_change='fail') }}

select c.currency_sk, c.source_currency_value, c.currency_code, c.currency_name,
count(*) as movies_total, count(f.amount) as movies_with_budget,
case when c.currency_code is not null then sum(f.amount)::numeric(30,2) end as budget_total,
case when c.currency_code is not null then avg(f.amount)::numeric(30,2) end as budget_average,
round(100.0 * count(f.amount) / nullif(count(*), 0), 2)::numeric(5,2) as budget_coverage_pct
from {{ ref('v_budget_current') }} f join {{ ref('dim_currency') }} c on c.currency_sk = f.currency_sk
group by c.currency_sk, c.source_currency_value, c.currency_code, c.currency_name
