Overview
========
- Project is made to model Gamestop-like enterprise, utilizing the most popular techniques to orchestrate data

Project Contents:
================

## Enhanced Lineage Graph

-------
  - Shows the entire data pipeline starting with MySQL, JSON and Static tables up to aggregated Mart layer and the relations between layers and models.

![image](https://github.com/markokapranov/assignment_3_dbt/blob/master/%D0%97%D0%BD%D1%96%D0%BC%D0%BE%D0%BA%20%D0%B5%D0%BA%D1%80%D0%B0%D0%BD%D0%B0%202026-04-20%20%D0%BE%2017.05.35.png)


 1.Data:
 ------
- Consists of 3 data sources:
  - .csv files: customers, discounts, employees, products, shops
  - Tables from Transactional DB (MySQL): payments, orders, order_items, sales, sales_items
  - JSON files responsible for storing of reviews

 2.Airflow
 ------
 - Consists of 3 dags: 
    - hourly_dag - processes data from MySQL database from tables payments, orders order_item, sales, sale_items, and runs seeds and stage models
    - json_reviews_dag - processes json files stored reviews_json directory, utilizes variable last_review_id to read only last files
    - daily_dbt_dag - runs mart models
- Consists of 4 variables:
    - last_review_id - responsible for incremental json read
    - last_payment_datetime - responsible for incremental read from payments table
    - last_orders_datetime - responsible for incremental read from orders, order_items table
    - last_sales_datetime - responsible for incremental read from sales, sales_items table

3.DBT 
------
  - Consists of 22 models 11 of which stage models 7 dimension and 5 fact models
  - Staging models format files and create stage models from JSON, MySQL, static data.
  - Mart models create joined data tables ready for full-fledge Business Intelligence tasks split into dimension and fact tables.
  - Analysis models use Mart models to answer popular BI tasks, such as:
  ### 1. Products with the lowest average user rating

| product_name   | avg_rating | rank |
|:---------------|-----------:|-----:|
| Determine      | 1.00       | 1    |
| Nor            | 1.67       | 2    |
| Nothing        | 1.80       | 3    |
| Sit            | 2.00       | 4    |
| Activity       | 2.50       | 5    |

 ### 2. Discounts with the lowest total revenue among all

| discount_name                                   |   total_revenue |
|:------------------------------------------------|----------------:|
| Profound contextually-based process improvement |            1907 |
| Horizontal directional matrices                 |            2662 |
| Self-enabling bi-directional task-force         |            3089 |
| Mandatory holistic encryption                   |            4156 |
| Right-sized homogeneous capacity                |            5709 |


 ### 3. Percentage of Cancellation by category and platform

| Category   | Platform           | Cancellation (%) |
|:-----------|:------------------|----------:|
| Console    | PC                | 70.59     |
| Console    | Nintendo Switch   | 68.97     |
| Console    | Xbox              | 65.22     |
| Accessory  | PlayStation       | 64.52     |
| Accessory  | Mobile            | 64.00     |
| Game       | Nintendo Switch   | 63.64     |
| Accessory  | Nintendo Switch   | 62.50     |
| Game       | Xbox              | 55.56     |
| Game       | PC                | 50.00     |
| Console    | PlayStation       | 48.00     |
| Game       | PlayStation       | 46.67     |
| Game       | Mobile            | 25.00     |
| Accessory  | Xbox              | 20.00     |

## 4. Testing
------
  - Tests include -unique, -not_null, -relationships: to, field.
  - Custom tests check non-negativity of core table values, such as discount_percentage, price, quantity, rating, total_amount etc.
