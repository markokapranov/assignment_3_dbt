with source as (select * from {{ ref('platforms_raw') }}),
     renamed as (select id as platform_id, name as plt_name, manufacturer
                 from source)
select *
from renamed