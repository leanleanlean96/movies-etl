select currency_sk from {{ ref('mart_budget_current_by_currency') }}
where movies_with_budget > movies_total or budget_coverage_pct not between 0 and 100
or (currency_code is null and (budget_total is not null or budget_average is not null))
union all
select currency_sk from {{ ref('mart_box_office_current_by_market_currency') }}
where movies_with_box_office > movies_total or box_office_coverage_pct not between 0 and 100
or (currency_code is null and (box_office_total is not null or box_office_average is not null))
