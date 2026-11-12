{{ config(materialized='incremental', incremental_strategy='reconcile', unique_key=['movie_id'], partition_keys=['movie_id'], on_schema_change='fail') }}

select s.*, m.name, m.production_year
from {{ ref('v_movie_scores_current') }} s
join {{ ref('v_movie_current') }} m on m.movie_id = s.movie_id
