/*
    This staging model narrows the registration timeseries data to only those fields we need
    and adds a field for Republican advantage.
*/

{{ config (
    materialized="table"
)}}

WITH registration_timeseries AS (
    SELECT
        Date,
        District_Type,
        District,
        REP_TOT,
        DEM_TOT,
        UAF_TOT,
        OTH_TOT,
        TOT,
        RTLA,
        REP_TOT - DEM_TOT AS REP_ADV
    FROM {{ source('co_voterfile', 'registration-timeseries') }}
)

SELECT
    *
FROM registration_timeseries
