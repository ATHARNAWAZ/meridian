-- Refund amount must never exceed the original order amount.
-- A violation indicates either a data entry error or a fraud pattern.

with returns as (
    select * from {{ ref('stg_returns') }}
),

orders as (
    select order_id, amount as order_amount from {{ ref('stg_orders') }}
),

violations as (
    select
        r.return_id,
        r.order_id,
        r.refund_amount,
        o.order_amount,
        r.refund_amount - o.order_amount as overage
    from returns r
    inner join orders o on r.order_id = o.order_id
    where r.refund_amount > o.order_amount
)

select * from violations
