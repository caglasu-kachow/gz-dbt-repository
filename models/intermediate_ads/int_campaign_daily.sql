select 
 date_date,
 SUM(impression) as impression,
 SUM(click) as click,
 SUM(ads_cost) as ads_cost
FROM {{ref("int_campaigns")}} 
group by date_date
order by date_date desc