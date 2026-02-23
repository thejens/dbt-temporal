-- Trips as "events" — sourced from BigQuery public bikeshare dataset
select
    trip_id as event_id,
    start_station_id as user_id,
    subscriber_type as event_type,
    start_time as event_timestamp
from {{ source('raw', 'bikeshare_trips') }}
where duration_minutes > 0
{% if target.name == 'dev' %}
  and start_time >= TIMESTAMP '2020-01-01'
{% endif %}
