import random
from faker import Faker
import csv
from datetime import datetime, timedelta

fake = Faker()

# ========================
# CONFIG
# ========================
NUM_CUSTOMERS = 200
NUM_PRODUCTS = 30
NUM_SHOPS = 20
NUM_EMPLOYEES = 50
NUM_ORDERS = 400
NUM_SALES = 400
NUM_DISCOUNTS = 5
NUM_REVIEWS = 200

# ========================
# HELPERS
# ========================
def random_date(days_back=365):
    return datetime.now() - timedelta(days=random.randint(0, days_back))

def format_date(dt):
    return dt.strftime("%Y-%m-%d")

def write_csv(filename, data):
    if not data:
        return
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=data[0].keys(),
            quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        writer.writerows(data)

def get_discount_id():
    return random.choices(
        [None] + list(range(1, NUM_DISCOUNTS + 1)),
        weights=[0.8] + [0.2 / NUM_DISCOUNTS] * NUM_DISCOUNTS
    )[0]

# ========================
# CUSTOMERS
# ========================
customers = [{
    'id': i,
    'first_name': fake.first_name(),
    'last_name': fake.last_name(),
    'email': fake.email(),
    'join_date': format_date(random_date()),
    'loyalty_points': random.randint(0, 1000)
} for i in range(1, NUM_CUSTOMERS + 1)]

# ========================
# PLATFORMS
# ========================
platform_names = ["PC", "PlayStation", "Xbox", "Nintendo Switch", "Mobile"]
platforms = [{
    'id': i + 1,
    'name': name,
    'manufacturer': fake.company()
} for i, name in enumerate(platform_names)]

# ========================
# PRODUCTS
# ========================
products = [{
    'id': i,
    'name': fake.word().capitalize(),
    'category': random.choice(['Game', 'Accessory', 'Console']),
    'price': round(random.uniform(10, 70), 2),
    'stock_qty': random.randint(0, 100),
    'release_date': format_date(random_date(1000)),
    'platform_id': random.randint(1, 5)
} for i in range(1, NUM_PRODUCTS + 1)]

# ========================
# EMPLOYEES (FIXED)
# ========================
roles = ['Cashier', 'Sales Associate', 'Support']
employees = []

# Create managers (1 per shop)
for i in range(1, NUM_SHOPS + 1):
    employees.append({
        'id': i,
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'role': 'Manager',
        'hire_date': format_date(random_date(1500)),
        'shop_id': i
    })

# Create remaining employees
for i in range(NUM_SHOPS + 1, NUM_EMPLOYEES + 1):
    employees.append({
        'id': i,
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'role': random.choice(roles),
        'hire_date': format_date(random_date(1500)),
        'shop_id': random.randint(1, NUM_SHOPS)
    })

# ========================
# SHOPS (FIXED)
# ========================
shops = [{
    'id': i,
    'name': f"{fake.city()} Games",
    'location': fake.address().replace("\n", ", "),
    'manager_id': i,  # guaranteed valid manager
    'opening_date': format_date(random_date(2000))
} for i in range(1, NUM_SHOPS + 1)]

# ========================
# DISCOUNTS
# ========================
discounts = []
for i in range(1, NUM_DISCOUNTS + 1):
    start = random_date(180)
    end = start + timedelta(days=random.randint(5, 30))

    discounts.append({
        'id': i,
        'name': fake.catch_phrase(),
        'discount_pct': random.randint(5, 30),
        'start_date': format_date(start),
        'end_date': format_date(end)
    })

# ========================
# ORDERS + ITEMS
# ========================
orders = []
order_items = []

for i in range(1, NUM_ORDERS + 1):
    user_id = random.randint(1, NUM_CUSTOMERS)
    total = 0

    items = random.sample(products, random.randint(1, 5))

    for item in items:
        qty = random.randint(1, 3)
        total += item['price'] * qty

        order_items.append({
            'id': len(order_items) + 1,
            'order_id': i,
            'product_id': item['id'],
            'quantity': qty,
            'price': item['price']
        })

    discount_id = get_discount_id()

    if discount_id:
        pct = next(d['discount_pct'] for d in discounts if d['id'] == discount_id)
        total *= (1 - pct / 100)

    orders.append({
        'id': i,
        'user_id': user_id,
        'order_date': format_date(random_date()),
        'total_amount': round(total, 2),
        'status': random.choice(['Delivered', 'Cancelled']),
        'discount_id': discount_id
    })

# ========================
# SALES + ITEMS
# ========================
sales = []
sales_items = []

for i in range(1, NUM_SALES + 1):
    user_id = random.randint(1, NUM_CUSTOMERS)
    employee = random.choice(employees)
    total = 0

    items = random.sample(products, random.randint(1, 5))

    for item in items:
        qty = random.randint(1, 3)
        total += item['price'] * qty

        sales_items.append({
            'id': len(sales_items) + 1,
            'sale_id': i,
            'product_id': item['id'],
            'quantity': qty,
            'price': item['price']
        })

    discount_id = get_discount_id()

    if discount_id:
        pct = next(d['discount_pct'] for d in discounts if d['id'] == discount_id)
        total *= (1 - pct / 100)

    sales.append({
        'id': i,
        'user_id': user_id,
        'employee_id': employee['id'],
        'sale_date': format_date(random_date()),
        'total_amount': round(total, 2),
        'discount_id': discount_id
    })

# ========================
# PAYMENTS
# ========================
payments = []
payment_id = 1
payment_types = ['Credit Card', 'PayPal', 'Gift Card', 'Cash']

def generate_payments(records, is_order=1):
    global payment_id

    for record in records:
        status = random.choices(['Completed', 'Failed'], weights=[0.8, 0.2])[0]

        payments.append({
            'id': payment_id,
            'order_id': record['id'],
            'amount': round(record['total_amount'], 2),
            'payment_type': random.choice(payment_types),
            'payment_date': record.get("order_date", record.get("sale_date")),
            'status': status,
            'is_order': is_order
        })

        payment_id += 1

generate_payments(orders, 1)
generate_payments(sales, 0)

# ========================
# REVIEWS
# ========================
reviews = [{
    'id': i,
    'product_id': random.choice(products)['id'],
    'user_id': random.choice(customers)['id'],
    'rating': random.randint(1, 5),
    'comment': fake.sentence(),
    'review_date': format_date(random_date(200))
} for i in range(1, NUM_REVIEWS + 1)]

# ========================
# WRITE FILES
# ========================
write_csv("seeds/customers_raw.csv", customers)
write_csv("seeds/products_raw.csv", products)
write_csv("seeds/shops_raw.csv", shops)
write_csv("seeds/employees_raw.csv", employees)
write_csv("seeds/orders_raw.csv", orders)
write_csv("seeds/order_items_raw.csv", order_items)
write_csv("seeds/sales_raw.csv", sales)
write_csv("seeds/sales_items_raw.csv", sales_items)
write_csv("seeds/payments_raw.csv", payments)
write_csv("seeds/discounts_raw.csv", discounts)
write_csv("seeds/reviews_raw.csv", reviews)
write_csv("seeds/platforms_raw.csv", platforms)

print("✅ Data generation complete!")