with source as (
    select * from iceberg_scan(
        's3://iceberg-warehouse/meridian/payments',
        allowed_extensions = ['.parquet']
    )
),

renamed as (
    select
        event_id                                as payment_id,
        order_id,
        user_id,
        cast(amount as decimal(12, 2))          as amount,
        coalesce(currency, 'EUR')               as currency,
        payment_method,
        status,
        gateway,
        to_timestamp(event_ts / 1000.0)        as event_ts,
        cast(event_ts / 1000 as date)          as event_date
    from source
    where event_id is not null
      and order_id is not null
)

select * from renamed
