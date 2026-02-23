-- Custom generic test: asserts all non-null values in a column are positive.
{% test is_positive(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
