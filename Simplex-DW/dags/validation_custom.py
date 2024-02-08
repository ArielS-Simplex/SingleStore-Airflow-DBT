import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timezone, timedelta
from functools import partial
from scripts.commons.consts import Consts as cnst
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.aws.AwsManager import AwsManager

snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
postgres_hook = PostgresHook(postgres_conn_id="postgres_default")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# create logger/notifier
env_config = Variable.get("env_config", deserialize_json=True)
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_MONITOR_PIPE)

ARGS = {
    "owner": "Airflow",
    "start_date": days_ago(2),
    "provide_context": True,
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    "on_failure_callback": partial(error_notifier.send_notification, cnst.ERROR),
    "keepalives": 1
}

dag = DAG(
    dag_id="validation_custom",
    default_args=ARGS,
    schedule="0 */5 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=100),
)


def get_sf_count(query, table_name):
    try:
        num_of_rows_in_target = int(snowflake_hook.get_first(query)[0])
    except Exception as e:
        print(f"SF: Failed to get rows for table {table_name} ------" + str(e))
    return num_of_rows_in_target


def get_pg_count(query, table_name):
    try:
        cursor = postgres_hook.get_conn().cursor()
        cursor.execute(query)
        num_of_rows_in_source = int(cursor.fetchone()[0])

    except Exception as e:
        print(f"failed to get rows for table {table_name} ------" + str(e))

    return num_of_rows_in_source


def check_table_difference(query, table_name):
    now_utc = "'"+datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')+"'"+'::timestamptz'

    query_part_1 = query.get("part_1")
    query_part_2 = query.get("part_2", '')
    utc_time = query.get("utc_time", False)

    if bool(utc_time):
        query = f"""{query_part_1}{now_utc}{query_part_2}"""
    else:
        query = f"""{query_part_1}{query_part_2}"""

    pg_count = get_pg_count(query, table_name)
    sf_count = get_sf_count(query, table_name)
    delta = pg_count - sf_count
    print('delta-', delta, '*************', table_name)
    if pg_count > sf_count:
        text = f"(Postgres table {table_name} has {delta} more rows than Snowflake table"
        error_notifier.send_general_notification(message_text=text)

    if pg_count < sf_count:
        text = f"(Postgres table {table_name} has {delta} less rows than Snowflake table"
        error_notifier.send_general_notification(message_text=text)

    return True


def pg_sf_validation(**context):
    print(env_config)
    config_content = AwsManager.get_config(folder="validation/", file_name='validation', file_type='yml')
    print(config_content)

    for table in config_content:
        for name, query in config_content[table].items():
            check_table_difference(query=query, table_name=name)


with dag:
    pg_sf_validation_task = PythonOperator(
        task_id=f"pg_sf_validation",
        python_callable=pg_sf_validation,
    )

pg_sf_validation_task
