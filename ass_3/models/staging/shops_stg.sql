with source as (select * from {{ ref('shops_raw') }}),
     renamed as (select id as shop_id, name as shop_name, manager_id, opening_date from source)
select *
from renamed;