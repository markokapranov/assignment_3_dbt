with source as (select * from {{ source('external_db', 'orders') }}),
     renamed as (select id as order_id,
                        user_id,
                        order_date,
                        total_amount,
                        status,
                        discount_id from source)
select *
from renamed