with contributions as (
    select
        *
    from {{ ref('stg_co_campaign_finance__contributions') }}
),

results as (
    select
        *,
        row_number() over (partition by election_year, CO_ID order by election_type asc) as row
    from {{ ref('stg_co_campaign_finance__results') }}
    where CO_ID is not null
),

contributions_results as (
    select
        RecordID,
        contributions.election_year,
        Jurisdiction,
        contributions.CO_ID,
        CommitteeType,
        CommitteeName,
        CandidateName,
        contributor_type,
        ContributionType,
        ReceiptType,
        ContributionAmount,
        ContributionDate,
        LastName,
        FirstName,
        MI,
        Suffix,
        Address1,
        Address2,
        City,
        State,
        Zip,
        Employer,
        Occupation,
        OccupationComments,
        Explanation,
        FiledDate,
        Amended,
        Amendment,
        AmendedRecordID,
        Electioneering,
        election_type,
        office_name,
        district_name,
        primary_party,
        candidate_party,
        votes,
        votes_rank,
        incumbent,
        is_self_funder
    from contributions
    left join results on contributions.CO_ID=results.CO_ID and contributions.election_year=results.election_year and results.row=1
    left join {{ ref('self_funding_mix') }} as self_funding_mix on contributions.CO_ID=self_funding_mix.CO_ID and results.election_year=self_funding_mix.election_year
)

select
    *
from contributions_results

