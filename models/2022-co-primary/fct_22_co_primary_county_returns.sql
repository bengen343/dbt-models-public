
/*
    2022 Colorado Primary Returns by Day.
*/


WITH PARTIES AS(
  SELECT * FROM(
    SELECT
      VOTER_ID,
      COUNTY,
      PARTY
    FROM {{ source('co_voterfile', '2022-primary-returns') }}
    WHERE RECEIVED != 'nan'
  )
  PIVOT(
    COUNT(VOTER_ID)
    FOR PARTY IN ('REP', 'DEM', 'UAF')
  )
),

PARTY_CHOICE AS(
  SELECT * FROM(
    SELECT
      VOTER_ID,
      COUNTY,
      VOTED_PARTY
    FROM {{ source('co_voterfile', '2022-primary-returns') }}
    WHERE RECEIVED != 'nan'
  ) 
  PIVOT(
    COUNT(VOTER_ID)
    FOR VOTED_PARTY IN ('REP', 'DEM')
  )
),

TOTAL AS(
  SELECT * FROM(
    SELECT
      COUNT(VOTER_ID) AS TOTAL,
      COUNTY,
    FROM {{ source('co_voterfile', '2022-primary-returns') }}
    WHERE RECEIVED != 'nan'
    GROUP BY COUNTY
  )
),

TABLE_VIEW AS(
  SELECT * FROM PARTIES
  LEFT JOIN TOTAL ON PARTIES.COUNTY=TOTAL.COUNTY
  LEFT JOIN PARTY_CHOICE ON PARTIES.COUNTY=PARTY_CHOICE.COUNTY
)

SELECT
  *
FROM TABLE_VIEW
