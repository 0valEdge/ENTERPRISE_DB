-- Singular test: lifetime_spend must equal lifetime_spend_pretax + lifetime_tax_paid.
-- Detects any rounding or calculation drift in the customers mart.

select
    customer_id,
    lifetime_spend,
    lifetime_spend_pretax,
    lifetime_tax_paid,
    lifetime_spend_pretax + lifetime_tax_paid as expected_lifetime_spend,
    abs(lifetime_spend - (lifetime_spend_pretax + lifetime_tax_paid)) as discrepancy
from {{ ref('customers') }}
where abs(lifetime_spend - (lifetime_spend_pretax + lifetime_tax_paid)) > 0.01
