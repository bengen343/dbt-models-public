with contribution_totals as (
    select
        CO_ID,
        election_year,
        sum(ContributionAmount) AS total_contributions
    from {{ ref('stg_co_campaign_finance__contributions') }}
    group by CO_ID, election_year
    order by election_year, CO_ID
),

self_contributions as (
    select
        CO_ID,
        election_year,
        sum(ContributionAmount) AS total_self_contributions
    from {{ ref('stg_co_campaign_finance__contributions') }}
    WHERE (contributor_type = 'Individual' OR contributor_type='Candidate') AND ContributionAmount > 1500 AND lower(CommitteeType) LIKE 'candidate%'
    group by CO_ID, election_year
),

loans as (
    select
        CO_ID,
        election_year,
        sum(LoanBalance) * -1 AS loan_balance
    from {{ ref ('stg_co_campaign_finance__loans') }}
    where LoanBalance < 0
    group by CO_ID, election_year
),

funding_mix as (
    select
        contribution_totals.CO_ID,
        contribution_totals.election_year,
        contribution_totals.total_contributions,
        self_contributions.total_self_contributions,
        loans.loan_balance,
    from contribution_totals
    left join self_contributions ON contribution_totals.CO_ID=self_contributions.CO_ID AND contribution_totals.election_year=self_contributions.election_year
    left join loans ON contribution_totals.CO_ID=loans.CO_ID AND contribution_totals.election_year=loans.election_year
),

funding_conversion as (
    select
        CO_ID,
        election_year,
        total_contributions,
        (CASE WHEN total_self_contributions IS NULL THEN 0 ELSE total_self_contributions END) AS total_self_contributions,
        (CASE WHEN loan_balance IS NULL THEN 0 ELSE loan_balance END) AS loan_balance
    from funding_mix
),

funding_calc as (
    select
        CO_ID,
        election_year,
        total_contributions,
        total_self_contributions,
        loan_balance,
        SAFE_DIVIDE(total_self_contributions + loan_balance, total_contributions) as self_percentage,
        (CASE WHEN SAFE_DIVIDE(total_self_contributions + loan_balance, total_contributions) > 0.33 then true else false end) as is_self_funder
    from funding_conversion
)

select
    *
from funding_calc