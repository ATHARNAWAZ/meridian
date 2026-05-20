with source as (
    select * from iceberg_scan(
        's3://iceberg-warehouse/meridian/clicks',
        allowed_extensions = ['.parquet']
    )
),

renamed as (
    select
        event_id                                as click_id,
        user_id,
        product_id,
        page,
        session_id,
        referrer,
        device_type,
        to_timestamp(event_ts / 1000.0)        as event_ts,
        cast(event_ts / 1000 as date)          as event_date
    from source
    where event_id is not null
)

select * from renamed
