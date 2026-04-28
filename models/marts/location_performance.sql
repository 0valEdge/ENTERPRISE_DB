with

location_performance as (
    select * from {{ ref('int_location_performance') }}
)

select * from location_performance
