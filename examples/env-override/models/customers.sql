-- Simple model that also demonstrates env_var() in SQL.
-- The output dataset is controlled by the workflow's env overrides.
select
    station_id,
    name as station_name,
    status,
    '{{ env_var("DEPLOYMENT_ENV", "development") }}' as source_env
from {{ source('raw', 'bikeshare_stations') }}
