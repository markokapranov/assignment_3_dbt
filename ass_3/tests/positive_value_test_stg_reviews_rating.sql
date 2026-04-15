
                  
SELECT *
FROM {{ ref('stg_reviews')}}
WHERE rating < 0
                  