with source as (select * from {{ ref('orders_raw') }}),
     renamed as (select id as order_id, user_id, order_date, total_amount, status, discount_id from source)
select *
from renamed;