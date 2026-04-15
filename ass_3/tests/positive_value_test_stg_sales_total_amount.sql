
                  
SELECT *
FROM {{ ref('stg_sales')}}
WHERE total_amount < 0
                  