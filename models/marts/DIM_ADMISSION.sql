with ADMISSIONS as (
    select * from {{ source('ACADEMIC_INFORMATION_SYSTEM','ADMISSIONS') }}
)

select * from ADMISSIONS