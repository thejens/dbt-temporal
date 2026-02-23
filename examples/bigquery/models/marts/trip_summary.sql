-- Incremental trip summary by station and day.
-- Demonstrates: is_incremental(), {{ this }}, merge strategy, unique_key,
--   on_schema_change, BigQuery partition_by + cluster_by, conditional audit columns.
{{ config(
    materialized='incremental',
    unique_key=['station_id', 'trip_date'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['incremental', 'trips'],
    partition_by={
        'field': 'trip_date',
        'data_type': 'date',
        'granularity': 'month'
    },
    cluster_by=['station_id']
) }}

with trip_data as (
    select
        start_station_id as station_id,
        date(start_time) as trip_date,
        count(*) as trip_count,
        count(distinct bike_id) as unique_bikes,
        avg(duration_minutes) as avg_duration_minutes,
        sum(duration_minutes) as total_duration_minutes,
        count(distinct subscriber_type) as subscriber_type_count,
        min(start_time) as first_trip_at,
        max(start_time) as last_trip_at
    from {{ ref('stg_trips') }}
    {% if is_incremental() %}
      where start_time > (
          select coalesce(max(last_trip_at), timestamp('2000-01-01'))
          from {{ this }}
      )
    {% endif %}
    group by start_station_id, date(start_time)
)

select
    station_id,
    trip_date,
    trip_count,
    unique_bikes,
    {{ safe_round('avg_duration_minutes', 1) }} as avg_duration_minutes,
    {{ safe_round('total_duration_minutes', 0) }} as total_duration_minutes,
    subscriber_type_count,
    first_trip_at,
    last_trip_at,
    {% if var('enable_audit_columns', false) %}
    {{ generate_audit_columns() }},
    {% endif %}
    current_timestamp() as _loaded_at
from trip_data
