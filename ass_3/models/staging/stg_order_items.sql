with source as (select * from {{ source('external_db', 'order_items') }}),
     renamed as (select id as item_id, order_id, product_id, quantity, price from source)
select *
from renamed