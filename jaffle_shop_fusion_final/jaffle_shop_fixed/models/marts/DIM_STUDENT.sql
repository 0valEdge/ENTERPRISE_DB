with STUDENTS as (
   select * from {{ source('ACADEMIC_INFORMATION_SYSTEM','STUDENTS') }}
)

select * from STUDENTS