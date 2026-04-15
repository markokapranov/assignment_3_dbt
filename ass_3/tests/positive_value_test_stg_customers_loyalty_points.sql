
                  
SELECT *
FROM {{ ref('stg_customers')}}
WHERE loyalty_points < 0
                  