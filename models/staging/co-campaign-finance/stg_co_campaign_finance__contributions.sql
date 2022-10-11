WITH contributions AS (SELECT
    (CASE
      WHEN MOD(EXTRACT(YEAR FROM ContributionDate), 2) = 0 THEN EXTRACT(YEAR FROM ContributionDate)
      ELSE EXTRACT(YEAR FROM ContributionDate) + 1
    END) AS election_year,
    (CASE
      WHEN REGEXP_CONTAINS(LOWER(ContributorType), r'individual') THEN 'Individual'
      ELSE ContributorType
    END) AS contributor_type,
    *
FROM {{ source('co_campaign_finance', 'contribution') }}
)

SELECT 
    *
FROM contributions
