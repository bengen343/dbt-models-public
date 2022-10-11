WITH results AS (SELECT
    *
FROM {{ source('co_campaign_finance', 'results') }}
)

SELECT 
    *
FROM results