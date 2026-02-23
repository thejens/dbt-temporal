-- Adapter dispatch example: formats station IDs differently per warehouse.
-- BigQuery uses FORMAT(), default falls back to CAST.
{% macro format_station_id(column_name) %}
    {{ return(adapter.dispatch('format_station_id')(column_name)) }}
{% endmacro %}

{% macro default__format_station_id(column_name) %}
    cast({{ column_name }} as string)
{% endmacro %}

{% macro bigquery__format_station_id(column_name) %}
    format('%05d', cast({{ column_name }} as int64))
{% endmacro %}
