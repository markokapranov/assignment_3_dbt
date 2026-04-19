import json
import os
import pandas as pd
import duckdb
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
import logging

from airflow.providers.mysql.hooks.mysql import MySqlHook

logger = logging.getLogger(__name__)

DUCKDB_PATH = "/usr/local/airflow/ass_3/dev.duckdb"
REVIEWS_JSON_DIR = "/usr/local/airflow/ass_3/reviews_json/"

# Для сетапу, створіть базу даних в МуСкьюелі туди пододавайте таблиці через дата візард, і потім відповідні коннекшени зробіть в аірфлоу
# ТБД доробити таски для переведення таблиць з майескьюелю до дакдібі,відповідно переписати ці моделі саме підсурси з дакдібі
# ОБОВ'ЯЗКОВО УСІ ДАННІ ПЕРЕВЕСТИ У ПОТРІБНИЙ ФОРМАТ ЧЕРЕЗ ПАНДАС М ЙОГО В ТАКОМУ ПЛАНІ БУДЕМО ВИКОРИСТОВУВАТИ !!!!
# ФОРМАТ ТАБЛИЦЬ ДИВІТЬСЯ У ВІДПОВІДНИХ СІДАХ
@dag(
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",
    catchup=False
)
def json_reviews_pipeline():

    @task
    def detect_new_review_ids():
        last_id = int(Variable.get("last_review_id")) # def 0
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
                     CREATE TABLE IF NOT EXISTS reviews
                     (
                         id          INTEGER PRIMARY KEY,
                         product_id  INTEGER,
                         user_id     INTEGER,
                         rating      INTEGER,
                         comment     VARCHAR(500),
                         review_date DATETIME
                     )
                     """)

        conn.register("df", df)
        conn.execute("""
                     INSERT INTO reviews
                     SELECT id, product_id, user_id, rating, comment, review_date
                     FROM df ON CONFLICT (id) DO
                     UPDATE SET
                         product_id = excluded.product_id,
                         user_id = excluded.user_id,
                         rating = excluded.rating,
                         comment = excluded.comment,
                         review_date = excluded.review_date
                     """)

        max_id = df['id'].max()
        Variable.set("last_review_id", max_id)
        logger.info(f"Loaded {len(df)} reviews. Last processed date = {max_id},")
        return

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /usr/local/airflow/ass_3 && dbt run --select tag:reviews-hourly --profiles-dir /usr/local/airflow/ass_3 --project-dir /usr/local/airflow/ass_3'
    )
    new_ids = detect_new_review_ids()
    reviews_df = load_new_jsons(new_ids)
    transform_and_load_to_duckdb(reviews_df, new_ids) >> run_dbt


json_reviews_pipeline()


