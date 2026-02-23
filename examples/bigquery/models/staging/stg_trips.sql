-- Staging: bikeshare trips from public BigQuery dataset.
-- Demonstrates: source(), target-conditional filtering, extract(), format_date().
select
    trip_id,
    subscriber_type,
    bike_id,
    start_time,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    duration_minutes,
    extract(year from start_time) as trip_year,
    extract(month from start_time) as trip_month,
    format_date('%A', date(start_time)) as trip_day_of_week
from {{ source('raw', 'bikeshare_trips') }}
where duration_minutes > 0
  and duration_minutes < 1440
{% if target.name == 'dev' %}
  and start_time >= timestamp('2020-01-01')
{% endif %}
