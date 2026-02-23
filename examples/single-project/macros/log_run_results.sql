-- on-run-end macro: logs summary of run results (tests shared run state)
{% macro log_run_results() %}
    {% if execute and results %}
        {% set ns = namespace(pass=0, error=0, skip=0) %}
        {% for result in results %}
            {% if result.status == 'pass' or result.status == 'success' %}
                {% set ns.pass = ns.pass + 1 %}
            {% elif result.status == 'error' %}
                {% set ns.error = ns.error + 1 %}
            {% else %}
                {% set ns.skip = ns.skip + 1 %}
            {% endif %}
        {% endfor %}
        {{ log("RUN RESULTS SUMMARY: " ~ ns.pass ~ " passed, " ~ ns.error ~ " errors, " ~ ns.skip ~ " skipped", info=True) }}
    {% endif %}
{% endmacro %}
