-- int_orders_operational
with first_step as (
    select 
            date_date,
            count(distinct(orders_id)) as nb_of_transactions,
            sum(revenue) as revenue,
            sum(margin) as margin,
            sum(operational_margin) as operational_margin,
            sum(purchase_cost) as purchase_cost,
            sum(shipping_fee) as shipping_fee,
            sum(ship_cost) as ship_cost,
            sum(logcost) as log_cost,
            sum(quantity) as quantity
    from {{ref('int_orders_operational')}}
    group by date_date
)
select
        date_date,
        nb_of_transactions,
        ROUND(revenue, 2) as revenue,
        ROUND(margin, 2) as margin,
        ROUND(operational_margin, 2) as operational_margin,
        ROUND(purchase_cost, 2) as purchase_cost,
        ROUND(shipping_fee, 2) as shipping_fee,
        ROUND(ship_cost, 2) as ship_cos,
        ROUND(log_cost, 2) as log_cost,
        ROUND(quantity, 2) as quantity,
        ROUND((revenue / nb_of_transactions), 2) as avg_basket
from first_step 
order by date_date desc
