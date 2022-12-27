/*
    This table aggregates the contribution data into totals by committee by election cycle.
*/

{{ config (
    materialized="table"
)}}

WITH contributions AS (
    SELECT
        election_year,
        CO_ID,
        CommitteeName,
        CommitteeType,
        CandidateName,
        contributor_type,
        COUNT(RecordID) AS unique_contributions,
        SUM(ContributionAmount) AS contribution_total,
        AVG(ContributionAmount) AS avg_contribution,
    FROM {{ ref('stg_co_campaign_finance__contributions') }}
    GROUP BY
        election_year,
        CO_ID,
        CommitteeName,
        CandidateName,
        contributor_type
)

SELECT
    *
FROM contributions
