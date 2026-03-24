with source as (select * from {{ ref('order_items_raw') }}),
     renamed as (select id as item_id, order_id, product_id, quantity, price from source)
select *
from renamed;