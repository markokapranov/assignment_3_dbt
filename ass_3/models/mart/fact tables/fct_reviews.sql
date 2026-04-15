{{ config(
    materialized='incremental',
    unique_key='review_id'
) }}
with reviews as (select *
                 from {{ ref('stg_reviews') }}),
     products as (select *
                  from {{ ref('dim_products') }}),
     users as (select *
               from {{ ref('dim_customers') }}),
     joined as (select r.review_id                                       as review_id,
                       p.product_name                                    as product_name,
                       {{string_joiner('u.first_name','u.last_name')  }} as user_name_surname,
                       r.rating                                          as rating,
                       r.comment                                         as comment,
                       r.review_date                                     as review_date,
                from reviews r
                         join products p on r.product_id = p.product_id
                         join users u on u.user_id = r.user_id)
select *
from joined
    {% if is_incremental() %}
where review_date > (select max(review_date) from {{ this }})
{% endif %}