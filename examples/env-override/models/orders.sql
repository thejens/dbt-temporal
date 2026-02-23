select
    trip_id,
    start_station_id,
    duration_minutes,
    start_time
from {{ source('raw', 'bikeshare_trips') }}
where duration_minutes > 0
