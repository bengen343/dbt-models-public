/*
    This mart gives the demographic district totals for the various Colorado political districts.
*/

{{ config (
    materialized="table"
)}}

WITH district_totals AS (
    SELECT
        congressional,
        county,
        state_senate,
        state_house,
        party,
        age_range,
        gender,
        race,
        pvg,
        pvp,
        COUNT(voter_id) AS voter_count
    FROM {{ ref('stg_co_sos__current_voters') }}
    GROUP BY
        congressional,
        county,
        state_senate,
        state_house,
        party,
        age_range,
        gender,
        race,
        pvg,
        pvp
),

contributions AS (
    SELECT
        election_year,
        office_name,
        district_name,
        (CASE
            WHEN office_name = 'State Senate' THEN 'State Senate ' || district_name
            WHEN office_name = 'State Representative' THEN 'State House ' || district_name
            ELSE office_name
        END) AS district,
        candidate_party,
        MAX(CASE
                WHEN election_type = 'General' THEN votes
                ELSE 0
            END) AS votes,
        SUM(ContributionAmount) AS total_raised
    FROM {{ ref('co_finance_committee_summary_stats') }}
    WHERE election_year = EXTRACT(YEAR FROM CURRENT_DATE()) + MOD(EXTRACT(YEAR FROM CURRENT_DATE()), 2) - 4
    GROUP BY
        election_year,
        office_name,
        district_name,
        candidate_party
)

SELECT
    *
FROM contributions
