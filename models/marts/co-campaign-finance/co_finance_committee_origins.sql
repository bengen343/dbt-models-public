with origins as (
    select
        election_year,
        CommitteeType,
        contributor_type,
        incumbent,
        count(RecordID) over (partition by election_year, CommitteeType, contributor_type, incumbent) as contributions,
        sum(ContributionAmount) over (partition by election_year, CommitteeType, contributor_type, incumbent) as total_contributions,
        percentile_cont(ContributionAmount, 0.5) over (partition by election_year, CommitteeType, contributor_type, incumbent) As median_contribution,
        avg(ContributionAmount) over (partition by election_year, CommitteeType, contributor_type, incumbent) as average_contribution,
        row_number() over (partition by election_year, CommitteeType, contributor_type, incumbent) as row
    from {{ ref('co_contributions') }}
    where CO_ID != 20175032139
    qualify row = 1
    order by election_year, CommitteeType, contributor_type
)

select 
    *
    except(row)
from origins
