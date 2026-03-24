with source as (select * from {{ ref('products_raw') }}),
     renamed as (select id as product_id, name as product_name, category, price, stock_qty, release_date, platform
                 from source)
select *
from renamed;