-- Daily station activity joining station metadata with trip summaries.
-- Demonstrates: execute guard, model/schema/database context, alias config,
--   post_hook with {{ this }}, ref() across materialization types.
{{ config(
    alias='station_daily_activity',
    tags=['reporting', 'daily'],
    post_hook=[
        "{{ log('POST-HOOK: built ' ~ this ~ ' (' ~ model.name ~ ') at ' ~ run_started_at.strftime('%H:%M:%S'), info=True) }}"
    ]
) }}

{% if execute %}
    {{ log('Compiling daily_station_activity: schema=' ~ schema ~ ', database=' ~ database, info=True) }}
{% endif %}

select
    s.station_id,
    s.station_name,
    s.status,
    t.trip_date,
    t.trip_count,
    t.avg_duration_minutes,
    t.total_duration_minutes,
    t.unique_bikes,
    t.first_trip_at,
    t.last_trip_at,
    {{ safe_round('t.trip_count * 1.0 / nullif(t.unique_bikes, 0)', 2) }} as trips_per_bike
from {{ ref('stg_stations_deduped') }} s
inner join {{ ref('trip_summary') }} t
    on s.station_id = t.station_id
