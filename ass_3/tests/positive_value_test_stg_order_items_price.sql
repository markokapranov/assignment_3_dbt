
                  
SELECT *
FROM {{ ref('stg_order_items')}}
WHERE price < 0
                  