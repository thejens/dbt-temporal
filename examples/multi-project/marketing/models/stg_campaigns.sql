-- Stations as "campaigns" — sourced from BigQuery public bikeshare dataset
select
    station_id as campaign_id,
    name as campaign_name,
    status as channel,
    0 as budget
from {{ source('raw', 'bikeshare_stations') }}
