-- Custom macro: wraps round() with a null guard
{% macro safe_round(field, precision=2) %}
    case
        when {{ field }} is not null then round({{ field }}, {{ precision }})
        else null
    end
{% endmacro %}
