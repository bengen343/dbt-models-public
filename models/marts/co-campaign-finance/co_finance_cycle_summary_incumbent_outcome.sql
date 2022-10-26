with cycle_level as (
  select
    election_year,
    election_type,
    office_name,
    incumbent,
    outcome,
    avg(contributions) as average_contributions,
    avg(total_contributions) as average_total_contributions,
    avg(average_contribution) as average_average_contribution,
    avg(expenditures) as average_expenditures,
    avg(total_expenditures) as average_total_expenditures,
    avg(average_expenditure) as average_average_expenditure
  from {{ ref('co_finance_committee_summary_stats') }}
  group by election_year, election_type, office_name, incumbent, outcome
  order by election_year, election_type, office_name, incumbent, outcome
)

select
    *
from cycle_level
