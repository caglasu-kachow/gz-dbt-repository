with sc as(
    select * from {{source('raw_ads', 'adwords')}}
)
select 
 date_date,
 paid_source,
 campaign_key,
 camPGN_name as campaign_name,
 CAST(ads_cost as FLOAT64) as ads_cost,
 CAST(impression as INT64) as impression,
 CAST(click as INT64) as click
from sc 