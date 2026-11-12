-- Materialize aggregate values before comparing: Cloudberry can mis-plan the
-- equivalent HAVING with several COUNT(DISTINCT ...) and OR predicates.
with stats as materialized (
    select snapshot_date_sk, count(*) as rows_count,
        count(distinct movie_id) as movies_count,
        count(distinct position) as positions_count,
        min(position) as first_position, max(position) as last_position
    from {{ ref('fact_top250_daily') }} group by snapshot_date_sk
)
select * from stats where rows_count <> 250 or movies_count <> 250
    or positions_count <> 250 or first_position <> 1 or last_position <> 250
