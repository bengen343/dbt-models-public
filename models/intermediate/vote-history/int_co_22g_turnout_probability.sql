
/*
    This table calculates the simple odds of a voter turning out to vote based on their
    voting behavoir in a given set of prior elections.
*/

{{ config (
    materialized="table"
)}}


/*
    The array election_lst must be populated manually with the dates of the last four General Elections
    preceding the election of interest. The current configuration is for the 2022 Colorado General.
*/

{% set election_lst = ['2014-11-04', '2016-11-08', '2018-11-06', '2020-11-03'] %}
{% set max_yr = '2020' %}


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
    WHERE CAST(election_date AS STRING) IN UNNEST( {{election_lst}} )
    GROUP BY voter_id, election_date
    ORDER BY voter_id, election_date ASC
),

vote_eligibility AS (
    SELECT
        voter_id,
        voted,
        DIV((({{max_yr}} + 2) - EXTRACT(YEAR FROM FIRST_VALUE(election_date) OVER(PARTITION BY voter_id ORDER BY election_date ASC))), 2) AS eligibility
    FROM binary_votes
),

vote_probability AS (
    SELECT
        voter_id,
        SAFE_DIVIDE(SUM(voted), eligibility) AS probability
    FROM vote_eligibility
    GROUP BY voter_id, eligibility
    ORDER BY voter_id ASC
)


SELECT
    *
FROM vote_probability
ORDER BY voter_id ASC
