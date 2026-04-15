{{ config(
    materialized='incremental',
    unique_key='item_id',
    tags = "daily"
) }}


with sales as (select *
               from {{ ref('stg_sales') }}),
     sales_items as (select *
                     from {{ ref('stg_sales_items') }}),
     platforms as (select *
                   from {{ ref('dim_platforms') }}),
     products as (select *
                  from {{ ref('dim_products') }}),
     shops as (select *
               from {{ ref('dim_shops') }}),
     users as (select * from {{ ref('dim_customers') }}),
     employees as (select * from {{ ref('dim_employees') }}),


     sales_joined as (select i.item_id                                      as item_id,
                             s.sale_id                                      as sale_id,
                             s.sale_date                                    as sale_date,
                             s.total_amount                                 as total_amount,
                             h.shop_name                                    as shop_name,
                             p.product_name                                 as product_name,
                             p.price                                        as price,
                             p.category                                     as category,
                             t.plt_name                                     as platform,
                             i.quantity                                     as quantity,
                             {{string_joiner('u.first_name','u.last_name')  }} as user_name_surname,
                            {{string_joiner('e.first_name','e.last_name')  }} as emp_name_surname

                      from sales_items i
                               join sales s on s.sale_id = i.sale_id
                               join products p on p.product_id = i.product_id
                               join platforms t on t.platform_id = p.platform_id
                               join users u on s.user_id = u.user_id
                               join employees e on e.employee_id = s.employee_id
                               join shops h on h.shop_id = e.shop_id
                     ),
    window_funcs as (
       select *,
        SUM(quantity) OVER (PARTITION BY product_name) as total_sold,
        SUM(quantity) OVER (PARTITION BY product_name, extract('year' FROM sale_date)) as sold_annual,
        SUM(quantity) OVER (PARTITION BY product_name, shop_name) as total_sold_shop,
        SUM(quantity) OVER (PARTITION BY product_name, emp_name_surname) as total_sold_emp,
        SUM(quantity) OVER (PARTITION BY product_name, user_name_surname) as total_bought_user

       from sales_joined

     )
       select * from window_funcs


{% if is_incremental() %}
where sale_date > (select max (sale_date) from {{ this }})
{% endif %}