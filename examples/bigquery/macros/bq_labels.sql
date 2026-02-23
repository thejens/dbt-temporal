-- Macro: generates BigQuery job labels as JSON for query comments.
-- Mirrors the aim-dbt-models bq_labels() pattern used in query-comment config.
-- Demonstrates: target, env_var(), invocation_id, tojson filter.
{% macro bq_labels() %}
    {%- set labels = {
        'dbt_invocation_id': invocation_id[:63],
        'dbt_target': target.name,
        'dbt_project': project_name | default(target.profile_name, true),
        'environment': env_var('DBT_ENV', 'dev'),
        'service': 'dbt-temporal'
    } -%}
    {{ return(tojson(labels)) }}
{% endmacro %}
