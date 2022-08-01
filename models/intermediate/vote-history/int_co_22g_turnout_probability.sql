
/*
    This table calculates the simple odds of a voter turning out to vote based on their
    voting behavoir in a given set of prior elections.
*/

{{ config (
    materialized="table"
)}}

DECLARE election_lst ARRAY <DATE>;
DECLARE max_yr INT64;

/*
    The array election_lst must be populated manually with the dates of the last four General Elections
    preceding the election of interest. The current configuration is for the 2022 Colorado General.
*/
SET election_lst = [DATE('2014-11-04'), DATE('2016-11-08'), DATE('2018-11-06'), DATE('2020-11-03')];
SET max_yr = EXTRACT(YEAR FROM election_lst[offset(3)]);


WITH vote_history AS(
    SELECT
        *
    FROM {{ ref('stg_co_sos__vote_history') }}
),

binary_votes AS (
    SELECT
        voter_id,
        election_date,
        COUNT(voter_id) AS voted
    FROM vote_history
    WHERE election_date IN UNNEST(election_lst)
    GROUP BY voter_id, election_date
    ORDER BY voter_id, election_date ASC
),

vote_eligibility AS (
    SELECT
        voter_id,
        voted,
        DIV(((max_yr + 2) - EXTRACT(YEAR FROM FIRST_VALUE(ElECTION_DATE) OVER(PARTITION BY VOTER_ID ORDER BY ElECTION_DATE ASC))), 2) AS eligibility
    FROM binary_votes
),

vote_probability AS (
    SELECT
        voter_id,
        SAFE_DIVIDE(SUM(VOTED), eligibility) AS probability
    FROM vote_eligibility
    GROUP BY voter_id, eligibility
    ORDER BY voter_id ASC
)


SELECT
    *
FROM vote_probability
ORDER BY voter_id ASC
LIMIT 10