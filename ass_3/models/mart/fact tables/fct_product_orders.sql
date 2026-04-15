{{ config(
    materialized='incremental',
    unique_key='item_id',
    tags = "hourly"
) }}


with orders as (select *
                from {{ ref('stg_orders') }}),
     order_items as (select *
                     from {{ ref('stg_order_items') }}),
     platforms as (select *
                   from {{ ref('dim_platforms') }}),
     products as (select *
                  from {{ ref('dim_products') }}),
     users as (select * from {{ ref('dim_customers') }}),


     sales_joined as (select i.item_id                                         as item_id,
                             o.order_id                                        as order_id,
                             o.order_date                                      as order_date,
                             o.total_amount                                    as total_amount,
                             p.product_name                                    as product_name,
                             p.price                                           as price,
                             p.category                                        as category,
                             t.plt_name                                        as platform,
                             i.quantity                                        as quantity,
                             {{string_joiner('u.first_name','u.last_name')  }} as user_name_surname,


                      from order_items i
                               join orders o on o.order_id = i.order_id
                               join products p on p.product_id = i.product_id
                               join platforms t on t.platform_id = p.platform_id
                               join users u on o.user_id = u.user_id),
     window_funcs as (select *,
                             SUM(quantity) OVER (PARTITION BY product_name) as total_sold, SUM(quantity) OVER (PARTITION BY product_name, extract('year' FROM order_date)) as sold_annual, SUM(quantity) OVER (PARTITION BY product_name, user_name_surname) as total_bought_user
                      from sales_joined)
select *
from window_funcs


    {% if is_incremental() %}
where order_date > (select max(order_date) from {{ this }})
{% endif %}