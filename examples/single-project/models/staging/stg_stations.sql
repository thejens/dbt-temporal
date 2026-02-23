-- Pulls from a public BigQuery dataset as a source
select
    station_id,
    name as station_name,
    status,
    number_of_docks
from {{ source('raw', 'bikeshare_stations') }}
