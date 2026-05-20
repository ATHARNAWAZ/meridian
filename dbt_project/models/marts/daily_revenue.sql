-- Daily revenue by category and country.
-- The primary mart for the revenue trend chart.

with order_payments as (
    select * from {{ ref('int_order_payments') }}
    where is_paid = true
),

aggregated as (
    select
        order_date,
        category,
        country,
        currency,
        count(distinct order_id)                         as order_count,
        count(distinct user_id)                          as unique_buyers,
        sum(order_amount)                                as gross_revenue,
        sum(case when is_fraud then order_amount else 0 end) as fraud_revenue,
        sum(order_amount) - sum(case when is_fraud then order_amount else 0 end) as net_revenue,
        avg(order_amount)                                as avg_order_value,
        min(order_amount)                                as min_order_value,
        max(order_amount)                                as max_order_value,
        sum(quantity)                                    as total_units_sold
    from order_payments
    group by 1, 2, 3, 4
)

select
    {{ generate_surrogate_key(['order_date', 'category', 'country']) }} as daily_revenue_id,
    *
from aggregated
order by order_date desc, gross_revenue desc
