-- Post-hook macro: sets BigQuery table description with structured metadata.
-- Mirrors the aim-dbt-models pattern of embedding JSON metadata in table descriptions.
-- Demonstrates: this, target, invocation_id, run_started_at, env_var(), model context.
{% macro set_table_metadata() %}
    {% if execute %}
        {% set metadata = {
            'dbt_invocation_id': invocation_id,
            'dbt_run_started_at': run_started_at.strftime('%Y-%m-%dT%H:%M:%S'),
            'target': target.name,
            'project': target.project,
            'dataset': target.schema,
            'environment': env_var('DBT_ENV', 'dev'),
            'model': model.name,
            'materialized': model.config.materialized
        } %}
        {% set description = tojson(metadata) | replace("'", "\\'") %}
        alter table {{ this }} set options (description='{{ description }}')
    {% endif %}
{% endmacro %}
