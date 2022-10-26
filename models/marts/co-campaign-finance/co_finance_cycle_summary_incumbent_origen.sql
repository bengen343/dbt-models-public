with cycle_level as (
  select
    election_year,
    contributor_type,
    incumbent,
    avg(contributions) as average_contributions,
    avg(total_contributions) as average_total_contributions,
    avg(average_contribution) as average_average_contribution,
    avg(expenditures) as average_expenditures,
    avg(total_expenditures) as average_total_expenditures,
    avg(average_expenditure) as average_average_expenditure
  from {{ ref('co_contributions') }}
  group by election_year, contributor_type, incumbent
  order by election_year, contributor_type, incumbent
)

select
    *
from cycle_level