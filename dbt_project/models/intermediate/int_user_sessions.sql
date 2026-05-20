-- Sessionise click events.
-- A session boundary is defined as 30 minutes of inactivity.
-- Assigns a session_number per user based on click ordering.

with clicks as (
    select * from {{ ref('stg_clicks') }}
),

with_lag as (
    select
        *,
        lag(event_ts) over (
            partition by user_id
            order by event_ts
        ) as prev_click_ts
    from clicks
),

session_starts as (
    select
        *,
        case
            when prev_click_ts is null then 1
            when date_diff('minute', prev_click_ts, event_ts) > 30 then 1
            else 0
        end as is_session_start
    from with_lag
),

session_numbers as (
    select
        *,
        sum(is_session_start) over (
            partition by user_id
            order by event_ts
            rows between unbounded preceding and current row
        ) as session_number
    from session_starts
),

final as (
    select
        click_id,
        user_id,
        product_id,
        page,
        session_id,
        referrer,
        device_type,
        event_ts,
        event_date,
        session_number,
        -- Session-level aggregates
        count(*) over (
            partition by user_id, session_number
        ) as session_page_views,
        min(event_ts) over (
            partition by user_id, session_number
        ) as session_start_ts,
        max(event_ts) over (
            partition by user_id, session_number
        ) as session_end_ts
    from session_numbers
)

select * from final
