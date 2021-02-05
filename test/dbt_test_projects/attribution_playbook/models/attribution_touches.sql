with all_sessions as (

    select * from {{ ref('sessions') }} s

),

all_customer_conversions as (

    select * from {{ ref('customer_conversions') }}

),

sessions_before_conversion as (

    select
        all_sessions.*,
        all_customer_conversions.revenue,

        count(*) over (
            partition by all_sessions.customer_id
        ) as total_sessions,

        row_number() over (
            partition by all_sessions.customer_id
            order by all_sessions.started_at
        ) as session_index

    from all_sessions

    left join all_customer_conversions on (all_sessions.customer_id = all_customer_conversions.customer_id)

    where all_sessions.started_at <= all_customer_conversions.converted_at
        and all_sessions.started_at >= all_customer_conversions.converted_at - 30

),

with_points as (

    select
        with_points_src.*,
        revenue * first_touch_points as first_touch_revenue,
        revenue * last_touch_points as last_touch_revenue,
        revenue * forty_twenty_forty_points as forty_twenty_forty_revenue,
        revenue * linear_points as linear_revenue
    from (
        select
            sessions_before_conversion.*,

            case
                when session_index = 1 then 1.0
                else 0.0
            end as first_touch_points,

            case
                when session_index = total_sessions then 1.0
                else 0.0
            end as last_touch_points,

            case
                when total_sessions = 1 then 1.0
                when total_sessions = 2 then 0.5
                when session_index = 1 then 0.4
                when session_index = total_sessions then 0.4
                else 0.2 / (total_sessions - 2)
            end as forty_twenty_forty_points,

            1.0 / total_sessions as linear_points

        from sessions_before_conversion
    ) with_points_src

)

select * from with_points
