{% test not_empty(model, column_name) %}

-- Generic test: fails if the model has zero rows.
-- Used on critical mart tables that must always have data.

select count(*) as row_count
from {{ model }}
having count(*) = 0

{% endtest %}
