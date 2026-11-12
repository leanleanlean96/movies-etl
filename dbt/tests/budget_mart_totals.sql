select 1 as mismatch where
(select coalesce(sum(movies_total), 0) from {{ ref('mart_budget_current_by_currency') }})
<> (select count(*) from {{ ref('v_budget_current') }})
