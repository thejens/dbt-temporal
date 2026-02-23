-- on-run-end macro: logs per-node results and a summary.
-- Demonstrates: results variable, execute guard, namespace, loop, node metadata.
{% macro log_run_results(results) %}
    {% if execute and results %}
        {% set ns = namespace(pass=0, error=0, skip=0) %}

        {{ log("========== Run Results ==========", info=True) }}
        {% for res in results %}
            {% if res.status == 'pass' or res.status == 'success' %}
                {% set ns.pass = ns.pass + 1 %}
            {% elif res.status == 'error' %}
                {% set ns.error = ns.error + 1 %}
                {{ log("  ERROR: " ~ res.node.unique_id ~ " — " ~ res.message, info=True) }}
            {% else %}
                {% set ns.skip = ns.skip + 1 %}
            {% endif %}
        {% endfor %}

        {{ log("SUMMARY: " ~ ns.pass ~ " passed, " ~ ns.error ~ " errors, " ~ ns.skip ~ " skipped | invocation=" ~ invocation_id, info=True) }}
        {{ log("=================================", info=True) }}
    {% endif %}
{% endmacro %}
