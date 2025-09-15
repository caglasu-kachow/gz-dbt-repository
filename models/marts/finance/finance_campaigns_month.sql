-- models/marts/finance/finance_campaigns_month.sql

with day as (
  
  select
    date_date,
    ads_cost,
    impression,      
    click,              
    quantity,
    revenue,
    purchase_cost,
    margin,
    shipping_fee,
    log_cost,
    ship_cos,        
    operational_margin,
    avg_basket
  from {{ ref('finance_campaigns_day') }}
),
orders_inferred as (
  select
    *,
    safe_divide(revenue, nullif(avg_basket, 0)) as orders_est
  from day
)

select
  date_trunc(date_date, month)                                       as datemonth,

 
  sum(coalesce(ads_cost, 0))                                         as ads_cost,
  sum(coalesce(impression, 0))                                       as ads_impressions,
  sum(coalesce(click, 0))                                            as ads_clicks,
  sum(coalesce(quantity, 0))                                         as quantity,
  sum(coalesce(revenue, 0))                                          as revenue,
  sum(coalesce(purchase_cost, 0))                                    as purchase_cost,
  sum(coalesce(margin, 0))                                           as margin,
  sum(coalesce(shipping_fee, 0))                                     as shipping_fee,
  sum(coalesce(log_cost, 0))                                         as log_cost,
  sum(coalesce(ship_cos, 0))                                        as ship_cost,
  sum(coalesce(operational_margin, 0))                               as operational_margin,

  -- Aylık ads_margin: toplam op_margin - toplam ads_cost
  sum(coalesce(operational_margin, 0)) - sum(coalesce(ads_cost, 0))  as ads_margin,

  -- Aylık average_basket: günlük ortalamalardan değil, bileşen toplamlarından
  round(
    safe_divide(
      sum(coalesce(revenue, 0)),
      nullif(sum(coalesce(orders_est, 0)), 0)
    )
  , 2)                                                               as average_basket

from orders_inferred
group by datemonth
order by datemonth desc
