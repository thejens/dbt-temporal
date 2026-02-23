-- Deduplicated stations: keep one row per station_id.
-- Demonstrates: ref(), window functions, subquery pattern.
select * except(_rn)
from (
    select
        *,
        row_number() over (partition by station_id order by station_name) as _rn
    from {{ ref('stg_stations') }}
)
where _rn = 1
