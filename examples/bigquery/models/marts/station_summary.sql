-- Station summary with comprehensive Jinja variable usage.
-- Demonstrates: var(), target, modules.datetime, safe_round macro,
--   pre_hook/post_hook in schema.yml, invocation_id, CTE-based lookups.
{{ config(
    tags=['reporting']
) }}

{% set current_ts = modules.datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') %}

with status_mapping as (
    select 'active' as status_code, 'Active' as status_label union all
    select 'closed', 'Closed' union all
    select 'moved', 'Relocated' union all
    select 'ACL', 'Access Controlled'
)

select
    s.status,
    coalesce(m.status_label, s.status) as status_label,
    count(*) as station_count,
    {{ safe_round('avg(s.station_id)', 0) }} as avg_station_id,
    '{{ var("shared_var") }}' as project_var,
    '{{ target.name }}' as target_name,
    '{{ target.project }}' as gcp_project,
    '{{ current_ts }}' as rendered_at_utc,
    '{{ invocation_id }}' as _dbt_invocation_id
from {{ ref('stg_stations_deduped') }} s
left join status_mapping m
    on s.status = m.status_code
group by s.status, m.status_label
