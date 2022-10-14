with stg_results as (
    select
        *
    from {{ ref('stg_co_campaign_finance__results') }}
),

self_funding as (
    select
        *
    from {{ ref('self_funding_mix')}}
),

results as (
    select
        stg_results.election_year,
        election_type,
        office_name,
        stg_results.CO_ID,
        primary_party,
        district_name,
        candidate_party,
        candidate_name,
        incumbent,
        votes,
        votes_rank,
        self_percentage,
        is_self_funder,
        total_self_contributions,
        loan_balance
    from stg_results
    left join self_funding on stg_results.CO_ID=self_funding.CO_ID and stg_results.election_year=self_funding.election_year
)

select
    *
from results
