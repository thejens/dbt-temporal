select
    s.station_id,
    s.station_name,
    count(t.trip_id) as total_trips,
    sum(t.duration_minutes) as total_duration
from {{ ref('customers') }} s
left join {{ ref('orders') }} t on s.station_id = t.start_station_id
group by s.station_id, s.station_name
