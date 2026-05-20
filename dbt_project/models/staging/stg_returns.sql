with source as (
    select * from iceberg_scan(
        's3://iceberg-warehouse/meridian/returns',
        allowed_extensions = ['.parquet']
    )
),

renamed as (
    select
        event_id                                as return_id,
        order_id,
        user_id,
        product_id,
        reason,
        cast(refund_amount as decimal(12, 2))   as refund_amount,
        to_timestamp(event_ts / 1000.0)        as event_ts,
        cast(event_ts / 1000 as date)          as event_date
    from source
    where event_id is not null
      and refund_amount >= 0
)

select * from renamed
