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


# HOURLY DAG

@dag(
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False
)
def hourly_pipeline():

    @task
    def detect_new_review_ids():
        last_id = Variable.get("last_rev_id")
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

        max_id = df['id'].max()
        Variable.set("last_rev_id", max_id)
        logger.info(f"Loaded {len(df)} reviews. Last processed id = {max_id}")
        return


    @task ### INSIDE WE WILL ENSURE DATA TYPES THAT THE WHOLE TRANSFORMATION, DISCOUNT ID NEEDS TRANSFORMATION CAUSE IT WAS PARSED AS TEXT
    #### PRIORLY SETUP CONNS IN AIRFLOW
    def mysql_to_duckdb_payments():
        last_call_datetime = pd.to_datetime(Variable.get("last_call_datetime")) # other variable
        hook = MySqlHook(mysql_conn_id='mysql_payments')
        query_call = f"SELECT * FROM payments';"
        df = hook.get_pandas_df(query_call)
        print("✅ Data extracted from M.")


        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute("""
                     
                     """)

        conn.execute("INSERT INTO payments SELECT * FROM df")
        conn.close()

        print("✅ Transformed data inserted into DuckDB.")

    @task
    def mysql_to_duckdb_orders():
        last_call_datetime = pd.to_datetime(Variable.get("last_call_datetime")) # other variable
        hook = MySqlHook(mysql_conn_id='mysql_orders')
        query_call = f"SELECT * FROM orders';"
        df = hook.get_pandas_df(query_call)
        print("✅ Data extracted from MySQL.")

        # Loading data into DuckDB
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

        print("✅ Transformed data inserted into DuckDB.")

    @task
    def mysql_to_duckdb_sales():
        last_call_datetime = pd.to_datetime(Variable.get("last_call_datetime")) # other variable
        hook = MySqlHook(mysql_conn_id='mysql_sales')
        query_call = f"SELECT * FROM sales';"
        df = hook.get_pandas_df(query_call)
        print("✅ Data extracted from MySQL.")

        # Loading data into DuckDB
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

        print("✅ Transformed data inserted into DuckDB.")

    @task
    def mysql_to_duckdb_sales_items():
        last_call_datetime = pd.to_datetime(Variable.get("last_call_datetime")) # other variable
        hook = MySqlHook(mysql_conn_id='mysql_sales_items')
        query_call = f"SELECT * FROM sales_items';"
        df = hook.get_pandas_df(query_call)
        print("✅ Data extracted from MySQL.")
        # Loading data into DuckDB
        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS sale_items
                     (
                         id
                         INTEGER
                         
                     );
                     """)

        conn.execute("INSERT INTO products SELECT * FROM df")
        conn.close()

        print("✅ Transformed data inserted into DuckDB.")

    @task
    def mysql_to_duckdb_order_items():
        last_call_datetime = pd.to_datetime(Variable.get("last_call_datetime")) # other variable
        hook = MySqlHook(mysql_conn_id='mysql_order_items')
        query_call = f"SELECT * FROM order_items';"
        df = hook.get_pandas_df(query_call)
        print("✅ Data extracted from MySQL.")
        # Loading data into DuckDB
        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS order_items
                     (
                         id
                         INTEGER
                     );
                     """)

        conn.execute("INSERT INTO products SELECT * FROM df")
        conn.close()

        print("✅ Transformed data inserted into DuckDB.")

    @task.bash
    def run_dbt():
        return "dbt run --select tags:hourly"

    new_ids = detect_new_review_ids()
    reviews_df = load_new_jsons(new_ids)
    transform_and_load_to_duckdb(reviews_df, new_ids)




hourly_pipeline()


# DAILY DAG

# HERE WE WILL ACTIVATEDBT DAILY MODELS TO PROVIDE DAILY ANALYSIS, DATA INGESTION WONT BE NEEDED BECAUSE HOYRLY DAG IS RESPONSIBLE FOR THIS
@dag(
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",
    catchup=False
)
def daily_pipeline():
    @task.bash
    def run_dbt():
        return "dbt run --select tags:daily"
    run_dbt()





daily_pipeline()