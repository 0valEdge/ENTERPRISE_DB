with course as (
    select * from {{ source('ACADEMIC_INFORMATION_SYSTEM','COURSES') }}
)

select * from course