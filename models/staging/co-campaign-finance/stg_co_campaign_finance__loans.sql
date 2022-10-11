WITH loans AS (SELECT
    (CASE
      WHEN MOD(EXTRACT(YEAR FROM LoanDate), 2) = 0 THEN EXTRACT(YEAR FROM LoanDate)
      ELSE EXTRACT(YEAR FROM LoanDate) + 1
    END) AS election_year,
    *
FROM {{ source('co_campaign_finance', 'loan') }}
)

SELECT 
    *
FROM loans
order by CO_ID, LoanDate
