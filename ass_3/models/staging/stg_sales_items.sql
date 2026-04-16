with source as (select * from {{ source('sales_items_raw') }}),
     renamed as (select id as item_id, sale_id, product_id, quantity, price from source)
select *
from renamed