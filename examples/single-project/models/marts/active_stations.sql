-- References the ephemeral model
select
    station_id,
    station_name
from {{ ref('ephemeral_helper') }}
where is_active
