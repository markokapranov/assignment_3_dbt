import json
import os
import glob
import pandas as pd
import duckdb
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

DUCKDB_PATH = "/usr/local/airflow/ass_3/dev.duckdb"
REVIEWS_JSON_DIR = "/usr/local/airflow/data/reviews_json"

@dag(
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",
    catchup=False
)
def pipeline():

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

        total = conn.execute("SELECT COUNT(*) FROM reviews_raw").fetchone()[0]
        logger.info(f"Total rows in reviews_raw: {total}")

    @task ### INSIDE WE WILL ENSUREDATA TYPES THAT THE WHOLE TRANSFORMATION
    def mysql_to_json_payments():
        return
    @task
    def mysql_to_json_orders():
        return

    @task
    def mysql_to_json_sales():
        return

    @task
    def mysql_to_json_sales_items():
        return

    @task
    def mysql_to_json_order_items():
        return

    @task.bash
    def activate_dbt():
        return
    new_ids = detect_new_review_ids()
    reviews_df = load_new_jsons(new_ids)
    transform_and_load_to_duckdb(reviews_df, new_ids)




pipeline()