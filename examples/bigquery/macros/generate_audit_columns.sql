-- Macro: adds standard audit columns to any model.
-- Uses invocation_id and run_started_at context variables.
{% macro generate_audit_columns() %}
    '{{ invocation_id }}' as _dbt_invocation_id,
    cast('{{ run_started_at.strftime("%Y-%m-%dT%H:%M:%S") }}' as timestamp) as _dbt_run_started_at
{% endmacro %}
