from airflow.sdk import dag, task, Variable
from pendulum import datetime, duration
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowFailException
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
import pandas as pd
import logging


@dag(
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": duration(minutes=2),
    },
    tags=["sales_pipeline"],
)
def sales_pipeline():

    @task
    def detect_new_sales():
        mysql_hook = MySqlHook(mysql_conn_id="my_sql_connection", schema="shop_db")

        watermark = Variable.get("sales_watermark", default="2000-01-01 00:00:00")
        logging.info(f"Current sales watermark: {watermark}")  # Observability
        sales_query = """
            SELECT
                id,
                user_id,
                employee_id,
                sale_date,
                total_amount,
                discount_id
            FROM sales
            WHERE sale_date >= %s
            ORDER BY sale_date
        """
        sales_records = mysql_hook.get_records(sales_query, parameters=(watermark,))

        if not sales_records:
            logging.info("No new sales detected.")  # Observability
            return {"sales": [], "payments": [], "watermark": watermark}

        logging.info(f"New sales detected: {len(sales_records)}")  # Observability

        sale_ids = [row[0] for row in sales_records]
        if len(sale_ids) != len(set(sale_ids)):
            raise AirflowFailException("Data quality check failed: duplicate IDs in sales records.")

        payments_query = """
            SELECT
                id,
                order_id,
                payment_date,
                amount,
                payment_type,
                status,
                is_order
            FROM payments
            WHERE payment_date >= %s
            ORDER BY payment_date
        """
        payments_records = mysql_hook.get_records(payments_query, parameters=(watermark,))
        logging.info(f"New payments detected: {len(payments_records)}")  # Observability


        payment_ids = [row[0] for row in payments_records]
        if len(payment_ids) != len(set(payment_ids)):
            raise AirflowFailException("Data quality check failed: duplicate IDs in payments records.")

        return {
            "sales":     sales_records,
            "payments":  payments_records,
            "watermark": watermark,
        }


    @task
    def load_sales_items(payload: dict):

        sales_records    = payload.get("sales", [])
        payments_records = payload.get("payments", [])
        watermark        = payload.get("watermark")

        if not sales_records:
            logging.info("No sales to process. Skipping sales_items fetch.")  # Observability
            return {"sales": [], "sales_items": [], "payments": [], "watermark": watermark}

        sale_ids     = [row[0] for row in sales_records]
        placeholders = ",".join(["%s"] * len(sale_ids))

        logging.info(f"Fetching sales_items for {len(sale_ids)} sales")  # Observability

        mysql_hook = MySqlHook(mysql_conn_id="my_sql_connection", schema="shop_db")

        items_query = f"""
            SELECT
                id,
                sale_id,
                product_id,
                quantity,
                price
            FROM sales_items
            WHERE sale_id IN ({placeholders})
        """
        items_records = mysql_hook.get_records(items_query, parameters=tuple(sale_ids))

        if not items_records:
            logging.warning("No sales_items found for the detected sales.")  # Observability

        logging.info(f"Sales items fetched: {len(items_records)}")  # Observability

        known_sale_ids = set(sale_ids)
        orphan_items   = [row for row in items_records if row[1] not in known_sale_ids]
        if orphan_items:
            raise AirflowFailException(
                f"Data quality check failed: {len(orphan_items)} sales_items "
                f"reference unknown sale IDs."
            )

        item_ids = [row[0] for row in items_records]
        if len(item_ids) != len(set(item_ids)):
            raise AirflowFailException("Data quality check failed: duplicate IDs in sales_items records.")

        return {
            "sales":      sales_records,
            "sales_items": items_records,
            "payments":   payments_records,
            "watermark":  watermark,
        }


    @task
    def transform_and_load_duckdb(payload: dict):

        sales_records  = payload.get("sales", [])
        items_records  = payload.get("sales_items", [])
        pay_records    = payload.get("payments", [])
        watermark      = payload.get("watermark")

        if not sales_records:
            logging.info("No data to load into DuckDB. Exiting.")  # Observability
            return

        sales_df = pd.DataFrame(
            sales_records,
            columns=["id", "user_id", "employee_id", "sale_date", "total_amount", "discount_id"],
        )
        sales_df = sales_df.astype({
            "id":           "int64",
            "user_id":      "int64",
            "employee_id":  "int64",
            "total_amount": "float64",
            "discount_id":  "string",
        })
        sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"])

        items_df = pd.DataFrame(
            items_records,
            columns=["id", "sale_id", "product_id", "quantity", "price"],
        )
        items_df = items_df.astype({
            "id":         "int64",
            "sale_id":    "int64",
            "product_id": "int64",
            "quantity":   "int64",
            "price":      "float64",
        })

        payments_df = pd.DataFrame(
            pay_records,
            columns=["id", "order_id", "payment_date", "amount", "payment_type", "status", "is_order"],
        )
        payments_df = payments_df.astype({
            "id":           "int64",
            "order_id":     "int64",
            "amount":       "float64",
            "payment_type": "string",
            "status":       "string",
            "is_order":     "int64",
        })
        payments_df["payment_date"] = pd.to_datetime(payments_df["payment_date"])

        logging.info(f"Sales DataFrame shape:       {sales_df.shape}")     # Observability
        logging.info(f"Sales items DataFrame shape: {items_df.shape}")     # Observability
        logging.info(f"Payments DataFrame shape:    {payments_df.shape}")  # Observability


        if sales_df["id"].duplicated().any():
            raise AirflowFailException("Data quality check failed: duplicate IDs in sales DataFrame.")

        if not items_df.empty and items_df["id"].duplicated().any():
            raise AirflowFailException("Data quality check failed: duplicate IDs in sales_items DataFrame.")

        if not payments_df.empty and payments_df["id"].duplicated().any():
            raise AirflowFailException("Data quality check failed: duplicate IDs in payments DataFrame.")

        if not items_df.empty and (items_df["quantity"] < 0).any():
            raise AirflowFailException("Data quality check failed: negative quantity in sales_items.")

        if not items_df.empty and (items_df["price"] < 0).any():
            raise AirflowFailException("Data quality check failed: negative price in sales_items.")

        if (sales_df["total_amount"] < 0).any():
            raise AirflowFailException("Data quality check failed: negative total_amount in sales.")

        if not payments_df.empty and (payments_df["amount"] < 0).any():
            raise AirflowFailException("Data quality check failed: negative amount in payments.")

        if not payments_df.empty and (~payments_df["is_order"].isin([0, 1])).any():
            raise AirflowFailException("Data quality check failed: is_order must be 0 or 1 in payments.")


        duckdb_hook = DuckDBHook(duckdb_conn_id="my_duckdb_connection")
        con = duckdb_hook.get_conn()

        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS sales (
                    id           INTEGER PRIMARY KEY,
                    user_id      INTEGER,
                    employee_id  INTEGER,
                    sale_date    TIMESTAMP,
                    total_amount DOUBLE,
                    discount_id  VARCHAR
                )
            """)

            con.execute("""
                CREATE TABLE IF NOT EXISTS sales_items (
                    id         INTEGER PRIMARY KEY,
                    sale_id    INTEGER,
                    product_id INTEGER,
                    quantity   INTEGER,
                    price      DOUBLE
                )
            """)

            con.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id           INTEGER PRIMARY KEY,
                    order_id     INTEGER,
                    payment_date TIMESTAMP,
                    amount       DOUBLE,
                    payment_type VARCHAR,
                    status       VARCHAR,
                    is_order     INTEGER
                )
            """)


            con.register("temp_sales", sales_df)
            con.execute("""
                INSERT INTO sales
                SELECT * FROM temp_sales
                ON CONFLICT (id) DO UPDATE SET
                    user_id      = excluded.user_id,
                    employee_id  = excluded.employee_id,
                    sale_date    = excluded.sale_date,
                    total_amount = excluded.total_amount,
                    discount_id  = excluded.discount_id
            """)
            logging.info(f"Rows upserted into sales: {len(sales_df)}")  # Observability


            if not items_df.empty:
                con.register("temp_sales_items", items_df)
                con.execute("""
                    INSERT INTO sales_items
                    SELECT * FROM temp_sales_items
                    ON CONFLICT (id) DO UPDATE SET
                        sale_id    = excluded.sale_id,
                        product_id = excluded.product_id,
                        quantity   = excluded.quantity,
                        price      = excluded.price
                """)
                logging.info(f"Rows upserted into sales_items: {len(items_df)}")  # Observability


            if not payments_df.empty:
                con.register("temp_payments", payments_df)
                con.execute("""
                    INSERT INTO payments
                    SELECT * FROM temp_payments
                    ON CONFLICT (id) DO UPDATE SET
                        order_id     = excluded.order_id,
                        payment_date = excluded.payment_date,
                        amount       = excluded.amount,
                        payment_type = excluded.payment_type,
                        status       = excluded.status,
                        is_order     = excluded.is_order
                """)
                logging.info(f"Rows upserted into payments: {len(payments_df)}")  # Observability


            latest_sale_date    = sales_df["sale_date"].max()
            latest_payment_date = (
                payments_df["payment_date"].max()
                if not payments_df.empty
                else pd.Timestamp(watermark)
            )
            new_watermark = max(latest_sale_date, latest_payment_date)

            Variable.set("sales_watermark", str(new_watermark))
            logging.info(f"Watermark updated to: {new_watermark}")  # Observability

        finally:
            con.close()

    new_sales    = detect_new_sales()
    enriched     = load_sales_items(new_sales)
    transform_and_load_duckdb(enriched)


sales_pipeline()