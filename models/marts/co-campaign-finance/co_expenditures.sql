with expenditures as (
    select
        *
    from {{ ref('stg_co_campaign_finance__expenditures') }}
),

results as (
    select
        *,
        row_number() over (partition by election_year, committee_id order by election_type asc) as row
    from {{ ref('stg_co_campaign_finance__results') }}
),

expenditures_results as (
    select
        RecordID,
        expenditures.election_year,
        Jurisdiction,
        expenditures.CO_ID,
        CommitteeType,
        CommitteeName,
        CandidateName,
        ExpenditureType,
        PaymentType,
        DisbursementType,
        ExpenditureAmount,
        ExpenditureDate,
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
    from expenditures
    left join results on expenditures.CO_ID=results.committee_id and expenditures.election_year=results.election_year and row=1
    left join {{ ref('self_funding_mix') }} as self_funding_mix on expenditures.CO_ID=self_funding_mix.CO_ID and results.election_year=self_funding_mix.election_year

)

select
   *
from expenditures_results
