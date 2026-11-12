with versions as (select *, lead(valid_from) over (partition by movie_id order by valid_from) as next_from from {{ ref('fact_movie_scores_history') }})
select * from versions where valid_to is distinct from next_from or valid_to <= valid_from
