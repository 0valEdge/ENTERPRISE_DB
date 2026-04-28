with Department as (
    select * from {{ source('ACADEMIC_INFORMATION_SYSTEM','DEPARTMENTS') }}
)

select * from Department