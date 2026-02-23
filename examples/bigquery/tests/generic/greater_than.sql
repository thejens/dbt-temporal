-- Custom generic test with arguments: asserts column values exceed a threshold.
-- Mirrors the aim-dbt-models numeric comparison tests.
{% test greater_than(model, column_name, threshold) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} <= {{ threshold }}

{% endtest %}
