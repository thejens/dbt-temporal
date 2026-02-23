-- Active stations enriched with trip metrics and audit metadata.
-- Demonstrates: ref() to ephemeral + ephemeral intermediate, invocation_id,
--   run_started_at, generate_audit_columns macro, meta config.
{{ config(
    tags=['core', 'stations'],
    meta={
        'owner': 'data-team',
        'contains_pii': false,
        'sla': 'daily'
    }
) }}

select
    s.station_id,
    {{ format_station_id('s.station_id') }} as station_id_formatted,
    s.station_name,
    s.status_label,
    sm.total_departures,
    sm.avg_trip_duration,
    sm.unique_bikes_used,
    sm.first_trip_at,
    sm.last_trip_at,
    {{ generate_audit_columns() }}
from {{ ref('ephemeral_helper') }} s
left join {{ ref('int_station_metrics') }} sm
    on s.station_id = sm.station_id
where s.is_active
