import json
import os
import pandas as pd
import duckdb
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
import logging

from airflow.providers.mysql.hooks.mysql import MySqlHook

logger = logging.getLogger(__name__)

DUCKDB_PATH = "/usr/local/airflow/ass_3/dev.duckdb"
REVIEWS_JSON_DIR = "/usr/local/airflow/data/reviews_json"

# Для сетапу, створіть базу даних в МуСкьюелі туди пододавайте таблиці через дата візард, і потім відповідні коннекшени зробіть в аірфлоу
# ТБД доробити таски для переведення таблиць з майескьюелю до дакдібі,відповідно переписати ці моделі саме підсурси з дакдібі
# ОБОВ'ЯЗКОВО УСІ ДАННІ ПЕРЕВЕСТИ У ПОТРІБНИЙ ФОРМАТ ЧЕРЕЗ ПАНДАС М ЙОГО В ТАКОМУ ПЛАНІ БУДЕМО ВИКОРИСТОВУВАТИ !!!!
# ФОРМАТ ТАБЛИЦЬ ДИВІТЬСЯ У ВІДПОВІДНИХ СІДАХ
@dag(
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False
)
def hourly_pipeline():

    @task
    def detect_new_review_ids():
        last_id = Variable.get("last_review_datetime")
        all_files = os.listdir(REVIEWS_JSON_DIR)
        new_ids = []
        for f in all_files:
            r_id = int(f.split('_')[1].split('.')[0])
            if r_id > last_id:
                new_ids.append(r_id)
        logger.info(f"New review ids found: {new_ids}")
        return new_ids

    @task
    def load_new_jsons(ids):
        if not ids:
            logger.info("No new review JSONs to load.")
            return None
        records = []
        for r_id in ids:
            filepath = os.path.join(REVIEWS_JSON_DIR, f"review_{r_id}.json")
            if not os.path.exists(filepath):
                logger.warning(f" {filepath} not found")
                continue
            with open(filepath, "r") as f:
                data = json.load(f)
                data['rating'] = data['rating']
                records.append(data)
        if not records:
            return None
        df = pd.DataFrame(records)
        df = df.sort_values('id').reset_index(drop=True)
        return df

    @task
    def transform_and_load_to_duckdb(df, ids):

        if df is None or df.empty:
            logger.info("No jsons to load.")
            return
        conn = duckdb.connect(DUCKDB_PATH)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS reviews_raw (
                id INTEGER PRIMARY KEY,
                product_id INTEGER,
                user_id INTEGER,
                rating INTEGER,
                comment VARCHAR(500),
                review_date DATETIME
            )
        """)

        conn.register("df", df)
        conn.execute("""
            INSERT OR REPLACE INTO reviews_raw
            SELECT id, product_id, user_id, rating, comment, review_date
            FROM df
        """)

        max_date = df['review_date'].max()
        Variable.set("last_review_datetime", max_date)
        logger.info(f"Loaded {len(df)} reviews. Last processed date = {max_date},")
        return


    @task
    def mysql_to_duckdb_payments():
        last_payment_datetime = pd.to_datetime(Variable.get("last_payment_datetime"))
        hook = MySqlHook(mysql_conn_id='mysql_payments')
        query_call = f"SELECT * FROM payments WHERE payment_date > '{last_payment_datetime}';"
        df = hook.get_pandas_df(query_call)
        logger.info("✅ Data extracted from MySQL- payments")


        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute("""
                    CREATE TABLE IF NOT EXISTS payments
                     (
                         id
                         INTEGER,
                         
                     );
                     
                     """)

        conn.execute("INSERT INTO payments SELECT * FROM df")
        conn.close()

        logger.info("✅ Transformed data inserted into DuckDB - payments.")

    @task
    def mysql_to_duckdb_orders():
        last_order_datetime = pd.to_datetime(Variable.get("last_order_datetime")) # other variable
        hook = MySqlHook(mysql_conn_id='mysql_orders')
        query_call = f"SELECT * FROM orders  WHERE order_date > '{last_order_datetime}';"
        df = hook.get_pandas_df(query_call)
        logger.info("✅ Data extracted from MySQL - orders.")


        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS orders
                     (
                         id
                         INTEGER
                     );
                     """)

        conn.execute("INSERT INTO orders SELECT * FROM df")
        conn.close()

        logger.info("✅ Transformed data inserted into DuckDB - orders.")


    @task
    def mysql_to_duckdb_order_items(order_ids):

        hook = MySqlHook(mysql_conn_id='mysql_order_items')
        query_call = f"SELECT * FROM order_items';"
        df = hook.get_pandas_df(query_call)
        logger.info("✅ Data extracted from MySQL - order_items.")

        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS order_items
                     (
                         id
                         INTEGER
                     );
                     """)

        conn.execute("INSERT INTO order_items SELECT * FROM df")
        conn.close()

        logger.info("✅ Transformed data inserted into DuckDB - order_items.")

    @task.bash
    def run_dbt():
        return "dbt run --select tags:hourly"

    new_ids = detect_new_review_ids()
    reviews_df = load_new_jsons(new_ids)
    transform_and_load_to_duckdb(reviews_df, new_ids)




hourly_pipeline()



@dag(
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",
    catchup=False
)
def daily_pipeline():
    @task
    def mysql_to_duckdb_sales():
        last_sale_datetime = pd.to_datetime(Variable.get("last_sale_datetime"))  # other variable
        hook = MySqlHook(mysql_conn_id='mysql_sales')
        query_call = f"SELECT * FROM sales WHERE sale > '{last_sale_datetime}';"
        df = hook.get_pandas_df(query_call)
        logger.info("✅ Data extracted from MySQL - sales.")


        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS sales
                     (
                         id
                         INTEGER
                     );
                     """)

        conn.execute("INSERT INTO sales SELECT * FROM df")
        conn.close()

        logger.info("✅ Transformed data inserted into DuckDB - sales.")

    @task
    def mysql_to_duckdb_sales_items(sales_ids):
        hook = MySqlHook(mysql_conn_id='mysql_sales_items')
        query_call = f"SELECT * FROM sales_items';"
        df = hook.get_pandas_df(query_call)
        logger.info("✅ Data extracted from MySQL.")

        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS sale_items
                     (
                         id
                         INTEGER

                     );
                     """)

        conn.execute("INSERT INTO sale_items SELECT * FROM df")
        conn.close()

        logger.info("✅ Transformed data inserted into DuckDB.")
    @task.bash
    def run_dbt():
        return "dbt run --select tags:daily"
    run_dbt()





daily_pipeline()