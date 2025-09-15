
select 
    d.date_date,
    d.ads_cost,
    d.impression,
    d.click,
    f.revenue,
    f.margin,
    f.operational_margin,
    f.quantity,
    f.purchase_cost,
    f.shipping_fee,
    f.log_cost,
    f.ship_cos,
    f.avg_basket,
    ROUND((f.operational_margin - d.ads_cost), 2) as ads_margin
from {{ref("int_campaign_daily")}} as d 
full outer join {{ref("finance_days")}} as f 
using (date_date)