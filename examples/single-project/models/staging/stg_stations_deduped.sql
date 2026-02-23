-- Deduplicated stations: keep one row per station_id, ordered by station_name.
select
    station_id,
    station_name,
    status,
    number_of_docks
from (
    select
        station_id,
        station_name,
        status,
        number_of_docks,
        row_number() over (partition by station_id order by station_name) as _rn
    from {{ ref('stg_stations') }}
) sub
where _rn = 1
