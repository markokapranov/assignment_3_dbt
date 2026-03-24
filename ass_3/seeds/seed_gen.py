import random
from faker import Faker
import csv
from datetime import datetime, timedelta

fake = Faker()

NUM_CUSTOMERS = 200
NUM_PRODUCTS = 50
NUM_SHOPS = 20
NUM_EMPLOYEES = 80
NUM_ORDERS = 200
NUM_DISCOUNTS = 15
NUM_WISHLISTS = 30
NUM_REVIEWS = 100
NUM_PAYMENTS = 100
NUM_PLATFORMS = 5

# Helper functions
def random_date(start_days_ago=365):
    return datetime.now() - timedelta(days=random.randint(0, start_days_ago))

# Generate Customers
customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    customers.append({
        'id': i,
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.email(),
        'join_date': random_date().strftime("%Y-%m-%d"),
        'loyalty_points': random.randint(0, 1000)
    })

# Generate Platforms
platforms = ["PC", "PlayStation", "Xbox", "Nintendo Switch", "Mobile"]
platforms_data = [{'id': i+1, 'name': p, 'manufacturer': fake.company()} for i, p in enumerate(platforms)]

# Generate Products
products = []
for i in range(1, NUM_PRODUCTS + 1):
    products.append({
        'id': i,
        'name': fake.word().capitalize(),
        'category': random.choice(['Game', 'Accessory', 'Console']),
        'price': round(random.uniform(10, 70), 2),
        'stock_qty': random.randint(0, 100),
        'release_date': random_date(1000).strftime("%Y-%m-%d"),
        'platform': random.choice(platforms)
    })

# Generate Shops
shops = []
for i in range(1, NUM_SHOPS + 1):
    shops.append({
        'id': i,
        'name': f"{fake.city()} Games",
        'location': fake.address().replace("\n", ", "),
        'manager_id': random.randint(1, NUM_EMPLOYEES),
        'opening_date': random_date(2000).strftime("%Y-%m-%d")
    })

# Generate Employees
employees = []
roles = ['Cashier', 'Manager', 'Sales Associate', 'Support']
for i in range(1, NUM_EMPLOYEES + 1):
    employees.append({
        'id': i,
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'role': random.choice(roles),
        'hire_date': random_date(1500).strftime("%Y-%m-%d"),
        'shop_id': random.randint(1, NUM_SHOPS)
    })

# Generate Orders
orders = []
order_items = []
for i in range(1, NUM_ORDERS + 1):
    user_id = random.randint(1, NUM_CUSTOMERS)
    order_date = random_date(365)
    total_amount = 0
    num_items = random.randint(1, 5)
    items = random.sample(products, num_items)
    for item in items:
        quantity = random.randint(1, 3)
        total_amount += item['price'] * quantity
        order_items.append({
            'id': len(order_items)+1,
            'order_id': i,
            'product_id': item['id'],
            'quantity': quantity,
            'price': item['price']
        })
    orders.append({
        'id': i,
        'user_id': user_id,
        'order_date': order_date.strftime("%Y-%m-%d"),
        'total_amount': round(total_amount, 2),
        'status': random.choice(['Pending', 'Shipped', 'Delivered', 'Cancelled'])
    })

# Generate Payments
payments = []
payment_types = ['Credit Card', 'PayPal', 'Gift Card', 'Cash']
for i in range(1, NUM_PAYMENTS + 1):
    order = random.choice(orders)
    payments.append({
        'id': i,
        'order_id': order['id'],
        'amount': order['total_amount'],
        'payment_type': random.choice(payment_types),
        'payment_date': random_date(365).strftime("%Y-%m-%d"),
        'status': random.choice(['Completed', 'Pending', 'Failed'])
    })

# Generate Discounts
discounts = []
for i in range(1, NUM_DISCOUNTS + 1):
    product = random.choice(products)
    start_date = random_date(180)
    end_date = start_date + timedelta(days=random.randint(5, 30))
    discounts.append({
        'id': i,
        'product_id': product['id'],
        'discount_pct': random.randint(5, 50),
        'start_date': start_date.strftime("%Y-%m-%d"),
        'end_date': end_date.strftime("%Y-%m-%d")
    })

# Generate Reviews
reviews = []
for i in range(1, NUM_REVIEWS + 1):
    product = random.choice(products)
    user = random.choice(customers)
    reviews.append({
        'id': i,
        'product_id': product['id'],
        'user_id': user['id'],
        'rating': random.randint(1, 5),
        'comment': fake.sentence(),
        'review_date': random_date(200).strftime("%Y-%m-%d")
    })

# Generate Wishlists
wishlists = []
for i in range(1, NUM_WISHLISTS + 1):
    user = random.choice(customers)
    product = random.choice(products)
    wishlists.append({
        'id': i,
        'user_id': user['id'],
        'product_id': product['id'],
        'added_date': random_date(200).strftime("%Y-%m-%d")
    })

# Function to write CSV
def write_csv(filename, data):
    if not data:
        return
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

# Write all CSVs
write_csv("customers_raw.csv", customers)
write_csv("products_raw.csv", products)
write_csv("shops_raw.csv", shops)
write_csv("employees_raw.csv", employees)
write_csv("orders_raw.csv", orders)
write_csv("order_items_raw.csv", order_items)
write_csv("payments_raw.csv", payments)
write_csv("discounts_raw.csv", discounts)
write_csv("reviews_raw.csv", reviews)
write_csv("wishlists_raw.csv", wishlists)
write_csv("platforms_raw.csv", platforms_data)

print("Data generation complete!")