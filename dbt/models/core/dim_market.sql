{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['market_sk'], partition_keys=['market_sk'], on_schema_change='fail') }}

select market_sk, market_code, market_name from {{ ref('market_mapping') }}
