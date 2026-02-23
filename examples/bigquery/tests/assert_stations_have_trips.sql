-- Singular test: every active station should have at least one recorded trip.
-- Returns failing rows (active stations with zero departures).
select
    s.station_id,
    s.station_name
from {{ ref('active_stations') }} s
left join {{ ref('trip_summary') }} t
    on s.station_id = t.station_id
where t.station_id is null
