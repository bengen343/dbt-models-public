with results as (
  select
    election_year,
    election_type,
    office_name,
    incumbent,
    (case when votes_rank = 1 then 'win' else 'loss' end) as outcome,
    count(*) as campaign_count
  from {{ ref('stg_co_campaign_finance__results') }}
  group by election_year, election_type, office_name, incumbent, outcome
  order by election_year, election_type, office_name, incumbent, outcome
),

aggregate as(  
  select
    election_year,
    election_type,
    office_name,
    incumbent,
    sum(campaign_count) over (partition by election_year, election_type, office_name, incumbent, outcome) / sum(campaign_count) over (partition by election_year, election_type, office_name, incumbent) as win_rate,
    row_number() over (partition by election_year, election_type, office_name, incumbent order by outcome desc) as row
  from results
  qualify row = 1
  order by election_year, election_type, office_name, incumbent
)

SELECT
  *
  except(row)
from aggregate