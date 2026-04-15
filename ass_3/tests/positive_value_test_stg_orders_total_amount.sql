
                  
SELECT *
FROM {{ ref('stg_orders')}}
WHERE total_amount < 0
                  