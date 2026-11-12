{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['currency_sk'], partition_keys=['currency_sk'], on_schema_change='fail') }}

select currency_sk, source_currency_value, currency_code, currency_name from {{ ref('currency_mapping') }}
