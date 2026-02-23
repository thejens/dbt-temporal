-- Daily active user counts
select
    date(event_timestamp) as activity_date,
    count(distinct user_id) as active_users
from {{ ref('stg_events') }}
group by date(event_timestamp)
