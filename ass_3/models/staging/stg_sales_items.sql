with source as (select * from {{  source('external_db', 'sales_items') }}),
     renamed as (select id as item_id, sale_id, product_id, quantity, price from source)
select *
from renamed