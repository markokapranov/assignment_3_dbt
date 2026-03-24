with source as (select * from {{ ref('wishlists_raw') }}),
     renamed as (select id as wishlist_id, user_id, product_id, added_date
                 from source)
select *
from renamed;