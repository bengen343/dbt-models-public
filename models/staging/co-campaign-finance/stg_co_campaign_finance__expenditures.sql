WITH expenditures AS (SELECT
    (CASE
      WHEN MOD(EXTRACT(YEAR FROM ExpenditureDate), 2) = 0 THEN EXTRACT(YEAR FROM ExpenditureDate)
      ELSE EXTRACT(YEAR FROM ExpenditureDate) + 1
    END) AS election_year,
    *
FROM {{ source('co_campaign_finance', 'expenditure') }}
)

SELECT 
    *
FROM expenditures