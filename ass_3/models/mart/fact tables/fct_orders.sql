{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_predicates = ["status = 'Completed'"]
) }}

with orders as (select *
                from {{ ref('stg_orders') }}),
     discounts as (select *
                   from {{ ref('dim_discounts') }}),
     payments as (select *
                  from {{ ref('stg_payments') }}),
     users as (select *
               from {{ ref('dim_customers') }}),
    joined as (
    select o.order_id as order_id,
           o.order_date as order_date,
           o.total_amount as total_amount,
           o.status as status,
           p.payment_type as payment_type,
           p.status as status,
           d.discount_name as discount_name,
           d.discount_pct as discount_percent,
           CONCAT(CONCAT(u.first_name,' '), u.last_name) as user_name_surname,
    from orders o
    join discounts d on d.discount_id = o.discount_id
    join payments p on p.order_id = o.order_id
    join users u on o.user_id = u.user_id
    where p.is_order = 1
)
select order_id,
       order_date,
       total_amount,
       status,
       discount_name,
       discount_percent
from joined
    {% if is_incremental() %}
where order_date > (select max(order_date) from {{ this }})
{% endif %}
