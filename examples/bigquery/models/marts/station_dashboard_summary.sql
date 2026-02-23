-- Dashboard-ready summary exercising advanced Jinja features.
-- Demonstrates: incremental model ref, target context, var(),
--   invocation_id, format_station_id dispatch, conditional dev limiting, CTE lookups.
{{ config(
    materialized='table',
    tags=['reporting', 'dashboard']
) }}

with status_mapping as (
    select 'active' as status_code, 'Active' as status_label, true as is_operational union all
    select 'closed', 'Closed', false union all
    select 'moved', 'Relocated', false union all
    select 'ACL', 'Access Controlled', true
)

select
    s.station_id,
    {{ format_station_id('s.station_id') }} as station_id_formatted,
    s.station_name,
    s.status,
    coalesce(t.trip_count, 0) as total_trips,
    coalesce(t.avg_duration_minutes, 0) as avg_duration_minutes,
    coalesce(t.unique_bikes, 0) as unique_bikes,

    -- Enrichment from inline status mapping
    coalesce(m.status_label, s.status) as status_label,
    coalesce(m.is_operational, false) as is_operational,

    -- Metadata from Jinja context
    '{{ target.name }}' as target_name,
    '{{ target.project }}' as gcp_project,
    '{{ invocation_id }}' as _dbt_invocation_id,
    '{{ var("shared_var") }}' as _project_var,
    {{ var('default_limit') }} as _configured_limit

from {{ ref('stg_stations_deduped') }} s
left join {{ ref('trip_summary') }} t
    on s.station_id = t.station_id
left join status_mapping m
    on s.status = m.status_code

{% if target.name == 'dev' %}
    -- dev: limit rows for faster iteration
    limit {{ var('default_limit') }}
{% endif %}
