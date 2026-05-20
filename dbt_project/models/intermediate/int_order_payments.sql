-- Join orders with their payment events.
-- An order may have multiple payment attempts (FAILED then SUCCESS).
-- We keep only the most recent payment per order.

with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

latest_payment_per_order as (
    select
        order_id,
        payment_id,
        amount         as payment_amount,
        currency       as payment_currency,
        payment_method,
        status         as payment_status,
        gateway,
        event_ts       as payment_ts,
        row_number() over (
            partition by order_id
            order by event_ts desc
        )              as rn
    from payments
),

final as (
    select
        o.order_id,
        o.user_id,
        o.product_id,
        o.category,
        o.amount       as order_amount,
        o.quantity,
        o.currency,
        o.country,
        o.event_ts     as order_ts,
        o.event_date   as order_date,
        o.is_fraud,
        p.payment_id,
        p.payment_amount,
        p.payment_method,
        p.payment_status,
        p.gateway,
        p.payment_ts,
        -- Was this order successfully paid?
        p.payment_status = 'SUCCESS'  as is_paid,
        -- Time from order to payment
        date_diff('second', o.event_ts, p.payment_ts) as payment_latency_seconds
    from orders o
    left join latest_payment_per_order p
        on o.order_id = p.order_id
        and p.rn = 1
)

select * from final
