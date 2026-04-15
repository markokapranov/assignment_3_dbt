with source as (select * from {{ ref('discounts_raw') }}),
     renamed as (select id as discount_id, discount_pct, name as discount_name, start_date, end_date
                 from source)
select *
from renamed