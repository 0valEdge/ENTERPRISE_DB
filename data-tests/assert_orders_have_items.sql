-- Singular test: Every order must have at least one order item.
-- If this query returns rows, the test fails.

select
    o.order_id
from {{ ref('orders') }} o
left join {{ ref('order_items') }} oi
    on o.order_id = oi.order_id
where oi.order_item_id is null
