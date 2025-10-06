from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests

# Configuration 
ALPHA_VANTAGE_API_KEY = Variable.get("vantage_api_key")  
SYMBOL = "MSFT"                                           
DATABASE = "USER_DB_TIGER"                                

# get Snowflake connection
SNOWFLAKE_CONN_ID = "snowflake_conn"
def get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn()

# Task 1: Extract last 90 days prices from API
@task
def return_last_90d_price():
    url = (
        f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY"
        f"&symbol={SYMBOL}&apikey={ALPHA_VANTAGE_API_KEY}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    results = []
    cutoff_date = datetime.now() - timedelta(days=90)
    for d, stock_info in data["Time Series (Daily)"].items():
        date_obj = datetime.strptime(d, "%Y-%m-%d")
        if date_obj >= cutoff_date:
            results.append((d, stock_info))   
    return results

# Task 2: Transform raw API output to structured records
@task
def transform_data(raw_data):
    records = []
    for date_str, daily_info in raw_data:
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            record = (
                SYMBOL,
                date_obj,
                float(daily_info["1. open"]),
                float(daily_info["4. close"]),
                float(daily_info["2. high"]),
                float(daily_info["3. low"]),
                int(daily_info["5. volume"])
            )
            records.append(record)
        except Exception as e:
            print(f"Skipping {date_str}: {e}")
    return records

# Task 3: Load into Snowflake using SQL transaction
@task
def load_data(records):
    target_table = f"{DATABASE}.raw.prices_etl"
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN;")
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {target_table} (
              symbol VARCHAR(10) NOT NULL,
              date DATE NOT NULL,
              open DECIMAL(12,4) NOT NULL,
              close DECIMAL(12,4) NOT NULL,
              high DECIMAL(12,4) NOT NULL,
              low DECIMAL(12,4) NOT NULL,
              volume BIGINT NOT NULL,
              PRIMARY KEY (symbol, date));"""
        )
        cursor.execute(f"DELETE FROM {target_table}")

        insert_sql = f"""
            INSERT INTO {target_table} (symbol, date, open, close, high, low, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_sql, records)

        cursor.execute("COMMIT;")
        print(f"Loaded {len(records)} rows into {target_table}")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(e)
        raise
    finally:
        cursor.close()
        conn.close()

# DAG definition
with DAG(
    dag_id="Homework5",
    start_date=datetime(2025, 10, 3),
    schedule="0 15 * * *",
    catchup=False,
    tags=["Microsoft stock"]
) as dag:

    raw = return_last_90d_price()
    transformed = transform_data(raw)
    load_data(transformed)