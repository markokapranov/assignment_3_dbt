
                  
SELECT *
FROM {{ ref('stg_discounts')}}
WHERE discount_pct < 0
                  