
                  
SELECT *
FROM {{ ref('stg_sales_items')}}
WHERE quantity < 0
                  