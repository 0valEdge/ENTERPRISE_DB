-- Customer RFM Scoring
-- Scores customers on Recency, Frequency, and Monetary value.
-- Used to segment customers for targeted marketing campaigns.

with

customers as (
    select * from {{ ref('customers') }}
),

-- compute raw RFM values
rfm_raw as (
    select
        customer_id,
        customer_name,
        customer_type,
        count_lifetime_orders                           as frequency,
        lifetime_spend                                  as monetary,
        last_ordered_at,

        -- days since last order (recency - lower is better)
         dbt_utils.datediff('day','last_ordered_at', 'current_date') 
            as days_since_last_order

    from customers
    where last_ordered_at is not null
),

-- assign 1-5 scores using ntile (5 = best)
rfm_scored as (
    select
        *,

        -- recency: fewer days = better = higher score
        6 - ntile(5) over (order by days_since_last_order asc)     as recency_score,

        -- frequency: more orders = better = higher score
        ntile(5) over (order by frequency asc)                     as frequency_score,

        -- monetary: more spend = better = higher score
        ntile(5) over (order by monetary asc)                      as monetary_score

    from rfm_raw
),

-- combine scores into a segment label
rfm_segmented as (
    select
        *,
        recency_score + frequency_score + monetary_score           as rfm_total_score,

        case
            when recency_score >= 4 and frequency_score >= 4
                then 'champion'
            when recency_score >= 3 and frequency_score >= 3
                then 'loyal_customer'
            when recency_score >= 4 and frequency_score <= 2
                then 'new_customer'
            when recency_score <= 2 and frequency_score >= 3
                then 'at_risk'
            when recency_score <= 2 and frequency_score <= 2
                then 'lost'
            else 'potential_loyalist'
        end                                                        as rfm_segment

    from rfm_scored
)

select * from rfm_segmented
