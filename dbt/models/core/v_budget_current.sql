{{ config(materialized='view') }}
select * from {{ ref('fact_budget_history') }} where valid_to is null
