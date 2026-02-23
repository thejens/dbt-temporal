-- Custom generic test: asserts that a model/seed/source is not empty.
-- Returns a row when the model has zero rows (= test failure in dbt).
{% test not_empty(model) %}

select 1 as empty_check
from (select count(*) as row_count from {{ model }}) t
where t.row_count = 0

{% endtest %}
