with committee_contributions as (
  select
    election_year,
    CommitteeType,
    election_type,
    office_name,
    incumbent,
    (case when votes_rank = 1 then 'win' else 'loss' end) as outcome,
    CO_ID,
    count(RecordID) as contributions,
    sum(ContributionAmount) as total_contributions,
    avg(ContributionAmount) as average_contribution
  from {{ ref('co_contributions') }}
  group by election_year, CommitteeType, election_type, office_name, incumbent, outcome, CO_ID
  order by election_year, CommitteeType, election_type, office_name, incumbent, outcome, CO_ID
),

committee_expenditures as (
  select
    election_year,
    CommitteeType,
    election_type,
    office_name,
    incumbent,
    (case when votes_rank = 1 then 'win' else 'loss' end) as outcome,
    CO_ID,
    count(RecordID) as expenditures,
    sum(ExpenditureAmount) as total_expenditures,
    avg(ExpenditureAmount) as average_expenditure
  from {{ ref('co_expenditures') }}
  group by election_year, CommitteeType, election_type, office_name, incumbent, outcome, CO_ID
  order by election_year, CommitteeType, election_type, office_name, incumbent, outcome, CO_ID
),

committee_level as (
  select
    committee_contributions.election_year,
    committee_contributions.CommitteeType,
    committee_contributions.election_type,
    committee_contributions.office_name,
    committee_contributions.incumbent,
    committee_contributions.outcome,
    committee_contributions.CO_ID,
    committee_contributions.contributions,
    committee_contributions.total_contributions,
    committee_contributions.average_contribution,
    committee_expenditures.expenditures,
    committee_expenditures.total_expenditures,
    committee_expenditures.average_expenditure
  from committee_contributions
  join committee_expenditures 
    on committee_contributions.election_year = committee_expenditures.election_year
    and committee_contributions.CO_ID = committee_expenditures.CO_ID
)

select
    *
from committee_level