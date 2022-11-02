
/*
    2022 Colorado General Returns by Day.
*/

{{ config (
    materialized="table"
)}}

WITH DAILY AS(
  SELECT * FROM(
    SELECT
      VOTER_ID,
      RECEIVED,
      PARTY
    FROM {{ source('co_voterfile', '2022-general-returns') }}
    WHERE RECEIVED != 'nan'
  )
  PIVOT(
    COUNT(VOTER_ID)
    FOR PARTY IN ('REP', 'DEM', 'UAF', 'OTH')
  )
)

SELECT
  *
FROM DAILY
ORDER BY RECEIVED ASC
