with source as (select * from {{ source('payments_raw') }}),
     renamed as (select id as payment_id, order_id, amount, payment_type, payment_date, status, is_order
                 from source)
select *
from renamed