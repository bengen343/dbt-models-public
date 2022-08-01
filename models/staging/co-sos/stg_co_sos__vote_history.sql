/*
    This table calculates the simple odds of a voter turning out to vote based on their
    voting behavoir in a given set of prior elections.
*/

{{ config (
    materialized="view"
)}}

WITH co_vote_history AS (
    SELECT
        VOTER_ID as voter_id,
        ELECTION_TYPE as election_type,
        CAST(ELECTION_DATE AS DATE) AS election_date,
        ELECTION_DESCRIPTION AS election_description,
        VOTING_METHOD AS voting_method,
        PARTY as party,
        COUNTY_NAME as county_name
    FROM {{ source('co_voterfile', 'vote-history') }}
)

SELECT
    *
FROM co_vote_history