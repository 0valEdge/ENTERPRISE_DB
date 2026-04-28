-- Singular test: No order should have a negative order total.
-- Negative totals would indicate a data pipeline issue.

select
    order_id,
    order_total
from {{ ref('orders') }}
where order_total < 0
