with source as (select *
                from {{ ref('employees_raw') }}),
     renamed as (select id as employee_id,
                        first_name,
                        last_name,
                        role,
                        hire_date,
                        shop_id
                 from source)
select *
from renamed