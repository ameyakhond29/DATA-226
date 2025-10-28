from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
from datetime import datetime

SNOWFLAKE_CONN_ID = "snowflake_conn"
DB_NAME = "USER_DB_TIGER"
RAW_SCHEMA = "RAW"
ANALYTICS_SCHEMA = "ANALYTICS"

@task
def create_session_summary():

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("BEGIN;")

        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_NAME}.{ANALYTICS_SCHEMA};")

        build_sql = f"""
            CREATE OR REPLACE TABLE {DB_NAME}.{ANALYTICS_SCHEMA}.session_summary AS
            WITH uc AS (
                SELECT userId, sessionId, channel
                FROM {DB_NAME}.{RAW_SCHEMA}.user_session_channel
                QUALIFY ROW_NUMBER() OVER (PARTITION BY sessionId ORDER BY userId) = 1
            ),
            st AS (
                SELECT sessionId, ts
                FROM {DB_NAME}.{RAW_SCHEMA}.session_timestamp
                QUALIFY ROW_NUMBER() OVER (PARTITION BY sessionId ORDER BY ts DESC) = 1
            )
            SELECT
                uc.userId,
                uc.sessionId,
                uc.channel,
                st.ts
            FROM uc
            INNER JOIN st USING (sessionId);
        """
        
        cursor.execute("COMMIT;")
        print(f"{DB_NAME}.{ANALYTICS_SCHEMA}.session_summary completed successfully.")

    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(f"Error while building session_summary. Transaction rolled back: {e}")
        raise AirflowException(f"ELT build failed: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@task
def check_for_duplicates():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    dup_count = 0
    try:
        cursor.execute("BEGIN;")
    # Bonus: SQL to find the count of sessionIds that appear more than once

        check_sql = f"""
    SELECT COUNT(*) FROM (
        SELECT sessionId
        FROM {DB_NAME}.{ANALYTICS_SCHEMA}.session_summary
        GROUP BY sessionId
        HAVING COUNT(*) > 1
    );
    """

        result = cursor.fetchone()
        if result and result[0] is not None:
            dup_count = result[0]

        cursor.execute("COMMIT;")

        print("No duplicate sessionId values found. Data quality check passed.")

    except AirflowException:
        raise
    except Exception as e:
        try:
            cursor.execute("ROLLBACK;")
        except Exception:
            pass 
        
        print(f"Error during DQ check query: {e}")
        raise AirflowException(f"DQ check task failed: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


with DAG(
    dag_id="ELT_DAG",
    start_date=datetime(2024, 10, 26),
    schedule="@daily",
    catchup=False,
    tags=["ELT", "Analytics", "Taskflow"],
) as dag:
    create_session_summary() >> check_for_duplicates()

