-- Singular test: rfm_total_score must always equal
-- recency_score + frequency_score + monetary_score.
-- Any row returned here indicates a calculation bug.

select
    customer_id,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_total_score,
    recency_score + frequency_score + monetary_score as expected_total
from {{ ref('customer_rfm_segments') }}
where rfm_total_score != recency_score + frequency_score + monetary_score
