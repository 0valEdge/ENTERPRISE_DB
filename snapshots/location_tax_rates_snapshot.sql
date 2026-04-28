{% snapshot location_tax_rates_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='location_id',
        strategy='check',
        check_cols=['tax_rate', 'location_name'],
        invalidate_hard_deletes=True
    )
}}

-- Tracks when store tax rates change over time.
-- Required for accurate historical tax reporting and finance reconciliation.
select
    location_id,
    location_name,
    tax_rate,
    opened_date
from {{ ref('stg_locations') }}

{% endsnapshot %}
