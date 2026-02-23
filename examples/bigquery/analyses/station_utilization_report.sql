-- Analysis: station utilization report.
-- Compiled by dbt but NOT executed — use the compiled output in BI tools.
-- Demonstrates: ref() in analyses, case expressions, left join pattern.
select
    s.station_id,
    s.station_name,
    s.status,
    coalesce(t.trip_count, 0) as total_trips,
    coalesce(t.avg_duration, 0) as avg_duration_minutes,
    case
        when coalesce(t.trip_count, 0) = 0 then 'unused'
        when t.trip_count < 100 then 'low'
        when t.trip_count < 1000 then 'medium'
        else 'high'
    end as utilization_tier
from {{ ref('stg_stations_deduped') }} s
left join (
    select
        start_station_id as station_id,
        count(*) as trip_count,
        avg(duration_minutes) as avg_duration
    from {{ ref('stg_trips') }}
    group by start_station_id
) t on s.station_id = t.station_id
order by total_trips desc
