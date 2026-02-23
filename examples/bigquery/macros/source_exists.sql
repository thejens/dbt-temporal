-- Utility macro: checks if a source table exists in the warehouse.
-- Mirrors the aim-dbt-models conditional source pattern.
-- Demonstrates: run_query(), execute guard, adapter.get_relation(), source().
{% macro source_exists(source_name, table_name) %}
    {% if execute %}
        {% set src = source(source_name, table_name) %}
        {% set relation = adapter.get_relation(
            database=src.database,
            schema=src.schema,
            identifier=src.identifier
        ) %}
        {{ return(relation is not none) }}
    {% else %}
        {{ return(true) }}
    {% endif %}
{% endmacro %}
