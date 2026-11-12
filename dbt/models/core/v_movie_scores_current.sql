{{ config(materialized='view') }}
select * from {{ ref('fact_movie_scores_history') }} where valid_to is null
