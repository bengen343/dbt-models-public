/*
    This mark creates a table of current voters disagregated by the different political districts they occupy.
*/

{{ config (
    materialized="table"
)}}

WITH current_voters AS (
    SELECT
        VOTER_ID AS voter_id,
        'Colorado' AS state,
        COUNTY AS county,
        CONGRESSIONAL AS congressional,
        STATE_SENATE AS state_senate,
        STATE_HOUSE AS state_house,
        PARTY AS party,
        AGE_RANGE AS age_range,
        GENDER AS gender,
        RACE AS race,
        PVG AS pvg,
    FROM {{ ref('stg_co_sos__current_voters') }}
),

counties AS (
    SELECT
        DISTINCT(COUNTY) AS district
    FROM {{ ref('stg_co_sos__current_voters') }}
),

congressionals AS (
    SELECT
        DISTINCT(CONGRESSIONAL) AS district
    FROM {{ ref('stg_co_sos__current_voters') }}
),

state_senates AS (
    SELECT
        DISTINCT(STATE_SENATE) AS district
    FROM {{ ref('stg_co_sos__current_voters') }}
),

state_houses AS (
    SELECT
        DISTINCT(STATE_HOUSE) AS district
    FROM {{ ref('stg_co_sos__current_voters') }}
),

districts AS (
    SELECT
        'Colorado' AS district
    UNION ALL
    SELECT
        *
    FROM congressionals
    UNION ALL
    SELECT
        *
    FROM counties
    UNION ALL
    SELECT
        *
    FROM state_senates
    UNION ALL
    SELECT
        *
    FROM state_houses
),

joined_districts AS (
    SELECT
        *
    FROM districts
    LEFT JOIN current_voters ON districts.district=current_voters.state
        OR districts.district=current_voters.county
        OR districts.district=current_voters.congressional
        OR districts.district=current_voters.state_senate
        OR districts.district=current_voters.state_house
)

SELECT
    *
FROM joined_districts