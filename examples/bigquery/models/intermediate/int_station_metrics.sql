-- Intermediate: aggregated trip metrics per station.
-- Demonstrates: ephemeral materialization (inlined as CTE in downstream models).
select
    start_station_id as station_id,
    count(*) as total_departures,
    avg(duration_minutes) as avg_trip_duration,
    count(distinct bike_id) as unique_bikes_used,
    min(start_time) as first_trip_at,
    max(start_time) as last_trip_at
from {{ ref('stg_trips') }}
group by start_station_id
