with source as (select * from {{  source('external_db', 'sales') }}),
     renamed as (select id as sale_id, user_id, sale_date,employee_id, total_amount, discount_id, from source)
select *
from renamed