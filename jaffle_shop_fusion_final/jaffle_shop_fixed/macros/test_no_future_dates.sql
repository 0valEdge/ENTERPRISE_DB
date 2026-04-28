{% test no_future_dates(model, column_name) %}

-- Generic test: fails if any value in the column is in the future.
-- Useful for catching bad timestamps from source systems.

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} > current_date

{% endtest %}
