-- Fraud alert summary aggregated for the dashboard.
-- Reads from Iceberg fraud_alerts topic data (written by Flink).

with orders as (
    select * from {{ ref('int_order_payments') }}
),

fraud_orders as (
    select
        order_date,
        category,
        country,
        count(distinct order_id)                   as fraud_order_count,
        count(distinct user_id)                    as fraud_user_count,
        sum(order_amount)                          as fraud_gmv,
        avg(order_amount)                          as avg_fraud_order_value
    from orders
    where is_fraud = true
    group by 1, 2, 3
),

total_orders as (
    select
        order_date,
        category,
        country,
        count(distinct order_id)                   as total_order_count,
        sum(order_amount)                          as total_gmv
    from orders
    group by 1, 2, 3
),

final as (
    select
        t.order_date,
        t.category,
        t.country,
        t.total_order_count,
        t.total_gmv,
        coalesce(f.fraud_order_count, 0)           as fraud_order_count,
        coalesce(f.fraud_user_count, 0)            as fraud_user_count,
        coalesce(f.fraud_gmv, 0)                   as fraud_gmv,
        coalesce(f.avg_fraud_order_value, 0)       as avg_fraud_order_value,
        round(100.0 * coalesce(f.fraud_order_count, 0) / nullif(t.total_order_count, 0), 3) as fraud_rate_pct
    from total_orders t
    left join fraud_orders f
        on t.order_date = f.order_date
        and t.category = f.category
        and t.country = f.country
)

select
    {{ generate_surrogate_key(['order_date', 'category', 'country']) }} as fraud_summary_id,
    *
from final
order by order_date desc, fraud_gmv desc
