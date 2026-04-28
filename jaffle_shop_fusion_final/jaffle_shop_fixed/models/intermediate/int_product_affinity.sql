-- Product Affinity Model
-- Identifies which products are most frequently ordered together.
-- Useful for cross-sell recommendations and combo meal analysis.

with

order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

-- self-join order_items to find co-occurring products in the same order
product_pairs as (
    select
        a.product_id                        as product_a_id,
        b.product_id                        as product_b_id,
        count(distinct a.order_id)          as co_occurrence_count
    from order_items a
    inner join order_items b
        on a.order_id = b.order_id
        and a.product_id < b.product_id    -- avoid duplicates and self-pairs
    group by 1, 2
),

-- join product names
enriched as (
    select
        pp.product_a_id,
        pa.product_name                     as product_a_name,
        pa.product_type                     as product_a_type,
        pp.product_b_id,
        pb.product_name                     as product_b_name,
        pb.product_type                     as product_b_type,
        pp.co_occurrence_count,

        -- rank pairs by how often they appear together
        rank() over (
            order by pp.co_occurrence_count desc
        )                                   as affinity_rank

    from product_pairs pp
    left join products pa on pp.product_a_id = pa.product_id
    left join products pb on pp.product_b_id = pb.product_id
)

select * from enriched
order by affinity_rank
