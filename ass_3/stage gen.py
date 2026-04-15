def write_stg(filename, query):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        print(filename)
        f.write(query)


customers_query = """
                  with source as (select *
                                  from {{ ref('customers_raw') }}),
                       renamed as (select id as user_id,
                                          first_name,
                                          last_name,
                                          email,
                                          join_date,
                                          loyalty_points
                                   from source)
                  select *
                  from renamed; \
 \
                  """

products_query = """
                 with source as (select * \
                                 from {{ ref('products_raw') }}),
                      renamed as (select id as product_id, \
                                         name as product_name, \
                                         category, \
                                         price, \
                                         stock_qty, \
                                         release_date, \
                                         platform \
                                  from source)
                 select * \
                 from renamed; \
 \
                 """
shops_query = """
              with source as (select * \
                              from {{ ref('shops_raw') }}),
                   renamed as (select id as shop_id, \
                                      name as shop_name , \
                                      manager_id, \
                                      opening_date \
                               from source)
              select * \
              from renamed; \
 \
              """
employees_query = """
                  with source as (select *
                                  from {{ ref('employees_raw') }}),
                       renamed as (select id as employee_id,
                                          first_name,
                                          last_name,
                                          role,
                                          hire_date,
                                          shop_id
                                   from source)
                  select *
                  from renamed; \
 \
                  """
orders_query = """
               with source as (select * \
                               from {{ ref('orders_raw') }}),
                    renamed as (select id as order_id, \
                                       user_id, \
                                       order_date, \
                                       total_amount, \
                                       status, \
                                       discount_id \
                                from source)
               select * \
               from renamed; \
 \
               """
order_items_query = """
                    with source as (select * \
                                    from {{ ref('order_items_raw') }}),
                         renamed as (select id as item_id, \
                                            order_id, \
                                            product_id, \
                                            quantity, \
                                            price \
                                     from source)
                    select * \
                    from renamed; \
 \
                    """
payments_query = """
                 with source as (select * \
                                 from {{ ref('payments_raw') }}),
                      renamed as (select id as payment_id, \
                                         order_id, \
                                         amount, \
                                         payment_type, \
                                         payment_date, \
                                         status
                                  from source)
                 select * \
                 from renamed; \
 \
                 """
discounts_query = """
                  with source as (select * \
                                  from {{ ref('discounts_raw') }}),
                       renamed as (select id as discount_id, \
                                          discount_pct, \
                                          start_date, \
                                          end_date
                                   from source)
                  select * \
                  from renamed; \
 \
                  """
platforms_query = """
                  with source as (select * \
                                  from {{ ref('platforms_raw') }}),
                       renamed as (select id as platform_id, \
                                          name as plt_name, \
                                          manufacturer
                                   from source)
                  select * \
                  from renamed; \
 \
                  """
reviews_query = """
                with source as (select * \
                                from {{ ref('reviews_raw') }}),
                     renamed as (select id as review_id, \
                                        product_id, \
                                        user_id, \
                                        rating, \
                                        comment, \
                                        review_date
                                 from source)
                select * \
                from renamed; \
 \
                """

write_stg("models/staging/stg_customers.sql", customers_query)
write_stg("models/staging/stg_products.sql", products_query)
write_stg("models/staging/stg_shops.sql", shops_query)
write_stg("models/staging/stg_employees.sql", employees_query)
write_stg("models/staging/stg_orders.sql", orders_query)
write_stg("models/staging/stg_order_items.sql", order_items_query)
write_stg("models/staging/stg_payments.sql", payments_query)
write_stg("models/staging/stg_discounts.sql", discounts_query)
write_stg("models/staging/stg_reviews.sql", reviews_query)
write_stg("models/staging/stg_platforms.sql", platforms_query)
