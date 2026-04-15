with source as (select *
                from {{ ref('customers_raw') }}),
     renamed as (select id as user_id,
                        first_name,
                        last_name,
                        email,
                        join_date,
                        loyalty_points
                 from source)
select *
from renamed