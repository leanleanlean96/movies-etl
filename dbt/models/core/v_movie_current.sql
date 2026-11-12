{{ config(materialized='view') }}
select *, true as is_current from {{ ref('dim_movie') }} where valid_to is null
