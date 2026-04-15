{{ config(
    materialized='incremental',
    unique_key='sale_id',
    incremental_predicates = ["payment_status = 'Completed'"]
) }}

with sales as (select *
               from {{ ref('stg_sales') }}),
     discounts as (select *
                   from {{ ref('dim_discounts') }}),
     payments as (select *
                  from {{ ref('stg_payments') }}),
     employees as (select *
                   from {{ ref('dim_employees') }}),
     shop as (select *
              from {{ ref('dim_shops') }}),
     users as (select *
               from {{ ref('dim_customers') }}),
    joined as (
    select s.sale_id as sale_id,
           s.sale_date as sale_date,
           s.total_amount as total_amount,
           d.discount_name as discount_name,
           d.discount_pct as discount_percent,
           h.shop_name as shop_name,
           p.payment_type as payment_type,
           p.status as payment_status,
           {{string_joiner('e.first_name','e.last_name')  }} as emp_name_surname,
           e.role as emp_role,
           {{string_joiner('u.first_name','u.last_name')  }} as user_name_surname
    from sales s
    join discounts d on d.discount_id = s.discount_id
    join payments p on p.order_id = s.sale_id
    join employees e on s.employee_id = e.employee_id
    join shop h on h.shop_id = e.shop_id
    join users u on s.user_id = u.user_id
    where p.is_order = 0
),
window_functions as(
  select *,
         CAST(SUM(total_amount) OVER (PARTITION BY shop_name) AS DECIMAL(10, 2)) as total_over_shops,
         CAST(SUM(total_amount) OVER (PARTITION BY emp_name_surname)AS DECIMAL(10, 2)) as total_over_employee
  from joined
)
select *
from window_functions

    {% if is_incremental() %}
where sale_date > (select max(sale_date) from {{ this }})
{% endif %}