-- Staging: orders from Iceberg via DuckDB iceberg extension
-- Casts types, renames columns, and adds derived fields.

with source as (
    select * from iceberg_scan(
        's3://iceberg-warehouse/meridian/orders',
        allowed_extensions = ['.parquet']
    )
),

renamed as (
    select
        event_id                                                    as order_id,
        user_id,
        product_id,
        category,
        cast(amount as decimal(12, 2))                              as amount,
        quantity,
        coalesce(currency, 'EUR')                                   as currency,
        country,
        -- Convert epoch ms to timestamp
        to_timestamp(event_ts / 1000.0)                            as event_ts,
        cast(event_ts / 1000 as date)                              as event_date,
        coalesce(is_fraud, false)                                   as is_fraud,
        coalesce(late_arrival, false)                              as late_arrival
    from source
    where event_id is not null
      and amount > 0
      and quantity > 0
)

select * from renamed
