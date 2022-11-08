
/*
    A model to tabulate the flow of voters changing parties between two time periods.
*/

WITH june21 AS (
    SELECT
        VOTER_ID,
        (CASE
            WHEN PARTY NOT IN ('REP', 'DEM', 'UAF') THEN 'OTH'
            ELSE PARTY
        END) AS PARTY_OLD
    FROM {{ source('co_voterfile', 'voters_20210601') }}
),

june22 AS (
    SELECT
        VOTER_ID,
        (CASE
            WHEN PARTY NOT IN ('REP', 'DEM', 'UAF') THEN 'OTH'
            ELSE PARTY
        END) AS PARTY_NEW
    FROM {{ source('co_voterfile', 'voters_20220601') }}
)

SELECT
    PARTY_OLD,
    PARTY_NEW,
    COUNT(june21.VOTER_ID) AS VOTERS
FROM june21
INNER JOIN june22 ON june21.VOTER_ID = june22.VOTER_ID
WHERE PARTY_OLD != PARTY_NEW
GROUP BY PARTY_OLD, PARTY_NEW
ORDER BY VOTERS DESC