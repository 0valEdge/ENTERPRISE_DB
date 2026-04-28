{% snapshot supply_costs_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='supply_uuid',
        strategy='check',
        check_cols=['supply_cost', 'supply_name', 'is_perishable_supply'],
        invalidate_hard_deletes=True
    )
}}

-- Tracks changes in supply costs over time.
-- Useful for understanding how input costs have shifted and their impact on margins.
select
    supply_uuid,
    supply_id,
    product_id,
    supply_name,
    supply_cost,
    is_perishable_supply
from {{ ref('stg_supplies') }}

{% endsnapshot %}
