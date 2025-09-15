-- int_orders_margin.sql
select
        o.orders_id,
        o.date_date,
        o.revenue,
        o.quantity,
        o.purchase_cost,
        o.margin,
        s.shipping_fee,
        s.ship_cost,
        s.logcost,
        ROUND((o.margin + s.shipping_fee) - (s.logcost + s.ship_cost), 2) AS operational_margin
from {{ref('int_orders_margin')}} AS o 
left join {{ref('stg_raw__ship')}} as s 
using (orders_id)      
    
