-- Staging: bikeshare stations from public BigQuery dataset.
-- Demonstrates: source(), var(), target context, env_var() with default.
select
    station_id,
    name as station_name,
    status,
    status = '{{ var("station_active_status") }}' as is_active,
    '{{ target.project }}' as _source_project,
    '{{ env_var("DBT_ENV", "dev") }}' as _environment
from {{ source('raw', 'bikeshare_stations') }}
