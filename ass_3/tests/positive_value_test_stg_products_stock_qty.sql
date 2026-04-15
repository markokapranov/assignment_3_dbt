
                  
SELECT *
FROM {{ ref('stg_products')}}
WHERE stock_qty < 0
                  