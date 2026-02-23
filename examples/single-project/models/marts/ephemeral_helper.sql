-- Tests ephemeral materialization — never hits the warehouse as a table
{{
    config(materialized='ephemeral')
}}

select
    station_id,
    station_name,
    case
        when status = 'active' then true
        else false
    end as is_active
from {{ ref('stg_stations') }}
