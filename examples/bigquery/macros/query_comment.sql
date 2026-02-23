-- Custom query comment macro: injects metadata into every BigQuery query.
-- Used via query-comment config in dbt_project.yml. BigQuery surfaces these
-- as job labels when job-label: true is set.
{% macro query_comment() %}
    {%- set comment_dict = {
        'app': 'dbt-temporal',
        'dbt_version': dbt_version,
        'profile': target.profile_name,
        'target': target.name,
        'schema': target.schema,
        'node_id': node.unique_id if node is defined and node else 'run-level'
    } -%}
    {{ return(tojson(comment_dict)) }}
{% endmacro %}
