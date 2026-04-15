def write_test(model, column):
    with open(f"tests/positive_value_test_{model}_{column}.sql", 'w', newline='', encoding='utf-8') as f:
        print(model)
        ref = "{{" + f" ref('{model}')" + "}}"
        query =f"""
                  
SELECT *
FROM {ref}
WHERE {column} < 0
                  """
        f.write(query)


write_test(model='stg_customers', column='loyalty_points')
write_test(model='stg_discounts', column='discount_pct')
write_test(model='stg_order_items', column='quantity')
write_test(model='stg_order_items', column='price')
write_test(model='stg_orders', column='total_amount')
write_test(model='stg_products', column='price')
write_test(model='stg_products', column='stock_qty')
write_test(model='stg_reviews', column='rating')
write_test(model='stg_sales', column='total_amount')
write_test(model='stg_sales_items', column='quantity')
write_test(model='stg_sales_items', column='price')