-- Aggregated user activity: event counts per user
select
    u.user_id,
    u.email,
    count(e.event_id) as total_events,
    min(e.event_timestamp) as first_event_at,
    max(e.event_timestamp) as last_event_at
from {{ ref('stg_users') }} u
left join {{ ref('stg_events') }} e on u.user_id = e.user_id
group by u.user_id, u.email
