-- int_campaigns.sql
select *
from {{ ref("stg_adwords") }}
union all
select *
from {{ ref("stg_bing") }}
union all
select *
from {{ ref("stg_facebook") }}
union all
select *
from {{ ref("stg_criteo") }}
