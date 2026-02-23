-- Tests ref(), custom macro, and var()
-- Also has pre-hook and post-hook configured in schema.yml

select
    status,
    count(*) as station_count,
    {{ safe_round('avg(number_of_docks)', 1) }} as avg_docks,
    '{{ var("shared_var") }}' as project_var
from {{ ref('stg_stations_deduped') }}
group by status
