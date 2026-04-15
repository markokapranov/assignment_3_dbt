COPY (with pre as (select product_name, AVG(rating)::DECIMAL(6,2) as avg_rating from fct_reviews GROUP BY product_name ORDER BY avg_rating ASC LIMIT 5),

            func as (select *, ROW_NUMBER() OVER (ORDER BY avg_rating ASC) as rank from pre)
            select * from func) TO 'top_product_to_eliminate.csv' (HEADER, DELIMITER ',');
COPY (select discount_name, SUM(total_amount)::INT64 as total_revenue  from fct_orders GROUP BY discount_name ORDER BY total_revenue) TO 'order_discount_analytics.csv' (HEADER, DELIMITER ",");