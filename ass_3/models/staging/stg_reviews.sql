with source as (select * from {{ source('reviews_raw') }}),
     renamed as (select id as review_id, product_id, user_id, rating, comment, review_date
                 from source)
select *
from renamed