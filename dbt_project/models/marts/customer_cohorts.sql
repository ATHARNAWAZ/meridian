-- Weekly cohort retention analysis.
-- Cohort = week of first order. Retention = active in subsequent weeks.

with orders as (
    select * from {{ ref('int_order_payments') }}
    where is_paid = true
),

first_order as (
    select
        user_id,
        date_trunc('week', min(order_ts)) as cohort_week
    from orders
    group by user_id
),

weekly_activity as (
    select
        o.user_id,
        fo.cohort_week,
        date_trunc('week', o.order_ts) as activity_week,
        date_diff('week', fo.cohort_week, date_trunc('week', o.order_ts)) as weeks_since_first
    from orders o
    inner join first_order fo on o.user_id = fo.user_id
),

cohort_counts as (
    select
        cohort_week,
        weeks_since_first,
        count(distinct user_id)         as active_users
    from weekly_activity
    group by 1, 2
),

cohort_sizes as (
    select
        cohort_week,
        count(distinct user_id)         as cohort_size
    from first_order
    group by 1
),

final as (
    select
        cc.cohort_week,
        cs.cohort_size,
        cc.weeks_since_first,
        cc.active_users,
        round(100.0 * cc.active_users / cs.cohort_size, 2) as retention_pct
    from cohort_counts cc
    inner join cohort_sizes cs on cc.cohort_week = cs.cohort_week
)

select
    {{ generate_surrogate_key(['cohort_week', 'weeks_since_first']) }} as cohort_id,
    *
from final
order by cohort_week desc, weeks_since_first
