-- Product performance: GMV, return rate, conversion.

with orders as (
    select * from {{ ref('int_order_payments') }}
    where is_paid = true
),

returns as (
    select * from {{ ref('stg_returns') }}
),

product_orders as (
    select
        product_id,
        category,
        count(distinct order_id)         as total_orders,
        count(distinct user_id)          as unique_buyers,
        sum(order_amount)                as gmv,
        avg(order_amount)                as avg_order_value,
        sum(quantity)                    as total_units_sold,
        sum(case when is_fraud then 1 else 0 end) as fraud_orders
    from orders
    group by 1, 2
),

product_returns as (
    select
        product_id,
        count(return_id)                 as return_count,
        sum(refund_amount)               as total_refunded
    from returns
    group by 1
),

final as (
    select
        po.product_id,
        po.category,
        po.total_orders,
        po.unique_buyers,
        po.gmv,
        po.avg_order_value,
        po.total_units_sold,
        po.fraud_orders,
        coalesce(pr.return_count, 0)     as return_count,
        coalesce(pr.total_refunded, 0)   as total_refunded,
        round(100.0 * coalesce(pr.return_count, 0) / nullif(po.total_orders, 0), 2) as return_rate_pct,
        po.gmv - coalesce(pr.total_refunded, 0)  as net_revenue
    from product_orders po
    left join product_returns pr on po.product_id = pr.product_id
)

select
    {{ generate_surrogate_key(['product_id']) }} as product_key,
    *
from final
order by gmv desc
