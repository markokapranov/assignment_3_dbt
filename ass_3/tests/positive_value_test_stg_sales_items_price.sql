
                  
SELECT *
FROM {{ ref('stg_sales_items')}}
WHERE price < 0
                  