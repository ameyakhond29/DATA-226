from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
from datetime import datetime

SNOWFLAKE_CONN_ID = "snowflake_conn"
DB_NAME = "USER_DB_TIGER"
ANALYTICS_SCHEMA = "analytics"
RAW_SCHEMA = "raw"

@task
def create_session_summary():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    sql = f"""
    CREATE OR REPLACE TABLE {DB_NAME}.{ANALYTICS_SCHEMA}.session_summary AS
    WITH t_dedup AS (
        SELECT *
        FROM (
            SELECT
                t.*,
                ROW_NUMBER() OVER (PARTITION BY t.sessionId ORDER BY t.ts DESC) AS rn
            FROM {DB_NAME}.{RAW_SCHEMA}.session_timestamp t
        )
        WHERE rn = 1
    ),
    c_dedup AS (
        SELECT *
        FROM (
            SELECT
                c.*,
                ROW_NUMBER() OVER (PARTITION BY c.sessionId ORDER BY c.sessionId) AS rn
            FROM {DB_NAME}.{RAW_SCHEMA}.user_session_channel c
        )
        WHERE rn = 1
    )
    SELECT
        t.sessionId AS session_id,
        c.userId AS user_id,
        t.ts AS session_timestamp,
        c.channel AS channel
    FROM t_dedup t
    JOIN c_dedup c
      ON t.sessionId = c.sessionId
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY session_id
        ORDER BY session_timestamp DESC
    ) = 1;
    """
    hook.run(sql)
    print("analytics.session_summary created (deduped) successfully.")

@task
def check_for_duplicates():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
# Extra point: Condition to check duplicate

    sql = f"""
    SELECT COUNT(*) AS dup_groups
    FROM (
        SELECT session_id
        FROM {DB_NAME}.{ANALYTICS_SCHEMA}.session_summary
        GROUP BY session_id
        HAVING COUNT(*) > 1
    );
    """
    rows = hook.get_records(sql)
    dup_groups = rows[0][0] if rows else 0
    if dup_groups and dup_groups > 0:
        raise AirflowException(f"Duplicate session_id groups found: {dup_groups}")
    print("No duplicate session_id rows found in analytics.session_summary.")

with DAG(
    dag_id="ELT_DAG",
    start_date=datetime(2024, 10, 23),
    schedule="@daily",  
    catchup=False,
    tags=["ELT","TaskFlow", "Analytics"],
) as dag:
    create_session_summary() >> check_for_duplicates()
