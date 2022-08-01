/*
    Table model simply matches the voter turnout probabilities into the full voter file.
*/

{{ config (
    materialized="view"
)}}

WITH co_voters AS (
    SELECT
        *   
    FROM {{ source('co_voterfile', 'co_voters_current') }}
),

turnout_probability AS (
    SELECT
        voter_id AS voter_id1,
        probability
    FROM {{ ref('int_co_22g_turnout_probability') }}
)

SELECT
    *
    EXCEPT (voter_id1)
FROM co_voters
LEFT JOIN turnout_probability ON co_voters.VOTER_ID=turnout_probability.voter_id1