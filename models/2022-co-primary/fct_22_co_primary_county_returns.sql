
/*
    2022 Colorado Primary Returns by Day.
*/


WITH PARTIES AS(
  SELECT * FROM(
    SELECT
      VOTER_ID,
      COUNTY AS PARTY_COUNTY,
      PARTY AS PARTY_PARTY
    FROM {{ source('co_voterfile', '2022-primary-returns') }}
    WHERE RECEIVED != 'nan'
  )
  PIVOT(
    COUNT(VOTER_ID)
    FOR PARTY_PARTY IN ('REP', 'DEM', 'UAF')
  )
),

PARTY_CHOICE AS(
  SELECT * FROM(
    SELECT
      VOTER_ID,
      COUNTY AS CHOICE_COUNTY,
      (CASE
          WHEN VOTED_PARTY = 'REP' THEN 'VOTED_REP'
          WHEN VOTED_PARTY = 'DEM' THEN 'VOTED_DEM'
        ELSE null
      END) AS VOTED_PARTY
    FROM {{ source('co_voterfile', '2022-primary-returns') }}
    WHERE RECEIVED != 'nan'
  ) 
  PIVOT(
    COUNT(VOTER_ID)
    FOR VOTED_PARTY IN ('VOTED_REP', 'VOTED_DEM')
  )
),

TOTAL AS(
  SELECT * FROM(
    SELECT
      COUNT(VOTER_ID) AS TOTAL,
      COUNTY AS TOTAL_COUNTY,
    FROM {{ source('co_voterfile', '2022-primary-returns') }}
    WHERE RECEIVED != 'nan'
    GROUP BY TOTAL_COUNTY
  )
),

TABLE_VIEW AS(
  SELECT * FROM PARTIES
  LEFT JOIN TOTAL ON PARTIES.PARTY_COUNTY=TOTAL.TOTAL_COUNTY
  LEFT JOIN PARTY_CHOICE ON PARTIES.PARTY_COUNTY=PARTY_CHOICE.CHOICE_COUNTY
)

SELECT
  PARTY_COUNTY AS COUNTY,
  REP,
  DEM,
  UAF,
  TOTAL,
  VOTED_REP,
  VOTED_DEM
FROM TABLE_VIEW
