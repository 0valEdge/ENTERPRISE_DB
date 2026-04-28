{% macro generate_composite_key(columns) %}

-- Generates a stable MD5 surrogate key from a list of columns.
-- Handles nulls by coalescing to empty string before hashing.
-- Example usage: {{ generate_composite_key(['order_id', 'product_id']) }}

    md5(
        concat_ws('|',
            {% for col in columns %}
                coalesce(cast({{ col }} as varchar), '')
                {%- if not loop.last %}, {% endif %}
            {% endfor %}
        )
    )

{% endmacro %}
