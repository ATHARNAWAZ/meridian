-- Orders must be unique by order_id in the staging layer.
-- Duplicates indicate a bug in the Iceberg sink or Flink exactly-once failure.

with order_counts as (
    select
        order_id,
        count(*) as cnt
    from {{ ref('stg_orders') }}
    group by order_id
)

select
    order_id,
    cnt as duplicate_count
from order_counts
where cnt > 1
