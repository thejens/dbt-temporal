-- Stations as "users" — sourced from BigQuery public bikeshare dataset
select
    station_id as user_id,
    name as email,
    status as created_at
from {{ source('raw', 'bikeshare_stations') }}
