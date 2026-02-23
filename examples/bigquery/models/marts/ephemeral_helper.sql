-- Ephemeral helper: station active flag with inline status mapping.
-- Demonstrates: ephemeral materialization, var(), CTE-based lookups.
{{
    config(materialized='ephemeral')
}}

with status_mapping as (
    select 'active' as status_code, 'Active' as status_label union all
    select 'closed', 'Closed' union all
    select 'moved', 'Relocated' union all
    select 'ACL', 'Access Controlled'
)

select
    s.station_id,
    s.station_name,
    s.status,
    coalesce(m.status_label, s.status) as status_label,
    s.status = '{{ var("station_active_status") }}' as is_active
from {{ ref('stg_stations') }} s
left join status_mapping m
    on s.status = m.status_code
