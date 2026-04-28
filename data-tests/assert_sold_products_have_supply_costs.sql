-- Singular test: No product should have zero total supply cost
-- if it has been sold at least once. Zero supply costs suggest
-- a missing supply record that could inflate margin reporting.

select
    oi.product_id,
    count(oi.order_item_id)     as times_ordered,
    sum(oi.supply_cost)         as total_supply_cost
from {{ ref('int_order_items_enriched') }} oi
group by 1
having
    count(oi.order_item_id) > 0
    and sum(oi.supply_cost) = 0
