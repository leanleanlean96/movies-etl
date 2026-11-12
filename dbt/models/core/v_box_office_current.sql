{{ config(materialized='view') }}
select * from {{ ref('fact_box_office_history') }} where valid_to is null
