from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_CONN_ID = "snowflake_conn"
DB_SCHEMA = "USER_DB_TIGER.raw"
STAGE_NAME = f"{DB_SCHEMA}.blob_stage"

with DAG(
    dag_id="Homework_6",
    start_date=datetime(2024, 10, 23),
    schedule='@daily',
    catchup=False,
    tags=["ETL", "Snowflake", "TaskFlow"],
) as dag:

    @task
    def create_objects():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN;")
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.user_session_channel (
                    userId VARCHAR(32) NOT NULL,
                    sessionId VARCHAR(32) PRIMARY KEY,
                    channel VARCHAR(32) DEFAULT 'direct'
                );
            """)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.session_timestamp (
                    sessionId VARCHAR(32) PRIMARY KEY,
                    ts TIMESTAMP
                );
            """)
            cursor.execute(f"""
                CREATE OR REPLACE STAGE {STAGE_NAME}
                URL = 's3://s3-geospatial/readonly/'
                FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            """)
            cursor.execute("COMMIT;")
            print("Snowflake objects (tables, stage) created successfully.")
        except Exception as e:
            cursor.execute("ROLLBACK;")
            print(f"Error in create_objects: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    @task
    def load_data():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN;")
            cursor.execute(f"""
                COPY INTO {DB_SCHEMA}.user_session_channel
                FROM @{STAGE_NAME}/user_session_channel.csv
            """)
            cursor.execute(f"""
                COPY INTO {DB_SCHEMA}.session_timestamp
                FROM @{STAGE_NAME}/session_timestamp.csv
            """)
            cursor.execute("COMMIT;")
            print("All raw data loaded successfully.")
        except Exception as e:
            cursor.execute("ROLLBACK;")
            print(f"Error in load_data: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    create_objects() >> load_data()