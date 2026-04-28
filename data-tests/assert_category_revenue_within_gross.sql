-- Singular test: Food revenue + drink revenue must never exceed gross revenue.
-- If it does, it means a product is being double-counted in both categories.

select
    order_date,
    location_id,
    gross_revenue,
    food_revenue,
    drink_revenue,
    food_revenue + drink_revenue as combined_category_revenue
from {{ ref('daily_revenue') }}
where food_revenue + drink_revenue > gross_revenue + 0.01  -- allow tiny rounding tolerance
