/*
    This staging model narrows our partitioned voter file down to only those voters who are currently active
    and those fields that we will use in subsequent models.
*/

{{ config (
    materialized="table"
)}}

WITH current_voters AS (
    SELECT
        VOTER_ID as voter_id,
        PRECINCT as precinct,
        COUNTY as county,
        LEFT(CONGRESSIONAL, LENGTH(CONGRESSIONAL)-2) || ' ' || FORMAT('%02d', CAST(SUBSTR(CONGRESSIONAL, -2) AS INT)) AS congressional,
        LEFT(STATE_SENATE, LENGTH(STATE_SENATE)-2) || ' ' || FORMAT('%02d', CAST(SUBSTR(STATE_SENATE, -2) AS INT)) AS state_senate,
        LEFT(STATE_HOUSE, LENGTH(STATE_HOUSE)-2) || ' ' || FORMAT('%02d', CAST(SUBSTR(STATE_HOUSE, -2) AS INT)) AS state_house,
        PARTY as party,
        AGE_RANGE as age_range,
        GENDER as gender,
        RACE as race,
        PVG as pvg,
        PVP as pvp
    FROM {{ source('co_voterfile', 'voters') }}
    WHERE VALID_TO_DATE IS NULL
)

SELECT
    *
FROM current_voters
