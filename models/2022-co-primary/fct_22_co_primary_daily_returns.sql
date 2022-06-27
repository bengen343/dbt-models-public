
/*
    2022 Colorado Primary Returns by Day.
*/

WITH DAILY AS(
  SELECT * FROM(
    SELECT
      VOTER_ID,
      RECEIVED,
      PARTY
    FROM {{ source('co_voterfile', '2022-primary-returns') }}
    WHERE RECEIVED != 'nan'
  )
  PIVOT(
    COUNT(VOTER_ID)
    FOR PARTY IN ('REP', 'DEM', 'UAF')
  )
)

SELECT
  *
FROM DAILY
ORDER BY RECEIVED ASC
