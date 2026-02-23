-- Trips as "impressions" — sourced from BigQuery public bikeshare dataset
select
    trip_id as impression_id,
    start_station_id as campaign_id,
    date(start_time) as impression_date,
    1 as clicks,
    case when duration_minutes > 30 then 1 else 0 end as conversions,
    round((duration_minutes * 0.10)::numeric, 2) as spend
from {{ source('raw', 'bikeshare_trips') }}
where duration_minutes > 0
{% if target.name == 'dev' %}
  and start_time >= TIMESTAMP '2020-01-01'
{% endif %}
