-- Revenue must never be negative.
-- This catches refund bugs or sign errors in the revenue pipeline.
-- Returns rows that FAIL the assertion (i.e., rows with negative revenue).

select
    daily_revenue_id,
    order_date,
    category,
    gross_revenue
from {{ ref('daily_revenue') }}
where gross_revenue < 0
