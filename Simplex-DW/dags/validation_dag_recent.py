from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.models import Variable
from airflow import DAG
import logging
import airflow

from functools import partial
from scripts.commons.consts import Consts as cnst
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.aws.AwsManager import AwsManager

# get config for AWS
black_list = AwsManager.get_config(folder="",
                                   file_name='tables_black_list',
                                   file_type='yml')

# create notifier
env_config = Variable.get("env_config", deserialize_json=True)
slack_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_MONITOR_PIPE)
VALIDATION_DAG_TIMEOUT = Variable.get("validation_dag_timeout")


MAX_TIME = "date_trunc('hour', current_timestamp ) - INTERVAL '2 hour'"
MIN_TIME = "date_trunc('year', current_timestamp ) - INTERVAL '1 year'"

snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
postgres_hook = PostgresHook(postgres_conn_id="postgres_default")


def get_sf_tables_list():
    try:
        table_list = snowflake_hook.get_records(
            f"SELECT table_schema, table_name, COLUMN_NAME FROM information_schema.columns WHERE COLUMN_NAME = 'CREATED_AT' OR COLUMN_NAME = 'INSERTED_AT'")

    except Exception as e:
        slack_notifier.send_general_notification(
            message_text='ERROR SF: Snowflake connection to retrieve tables list failed')
        logging.error(f"SF: failed to get tables. Reason: {e}")

    logging.info(f"Returned list of tables: {table_list}")
    return table_list


def get_sf_count(schema, table_name, order_field):
    num_of_rows_in_target = None
    try:
        num_of_rows_in_target = int(
            snowflake_hook.get_first(
                f"SELECT COUNT(*) FROM {schema}.{table_name} WHERE {order_field} BETWEEN {MIN_TIME} AND {MAX_TIME}")[0])

    except Exception as e:
        slack_notifier.send_general_notification(
            message_text=f'ERROR SF: Failed to get rows for table {schema}.{table_name}')
        raise Exception(f"ERROR SF: Failed to get rows for table {schema}.{table_name}. Reason: {e}")

    return num_of_rows_in_target


def get_pg_count(schema, table_name, order_field):
    try:
        cursor = postgres_hook.get_conn().cursor()
    except Exception as e:
        slack_notifier.send_general_notification(
            message_text=f'ERROR PG: Postgres connection failed for table {schema}.{table_name}')
        raise Exception(f"ERROR PG: Postgres connection failed for table {schema}.{table_name}. Reason: {e}")

    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM {schema}.{table_name} WHERE {order_field} BETWEEN {MIN_TIME} AND {MAX_TIME}")
        num_of_rows_in_source = int(cursor.fetchone()[0])

    except Exception as e:
        slack_notifier.send_general_notification(
            message_text=f'ERROR PG: Failed to get rows for table {schema}.{table_name}')
        logging.error(f"ERROR PG: Failed to get rows for table {schema}.{table_name}. Reason: {e}")
        raise Exception(f"ERROR PG: Failed to get rows for table {schema}.{table_name}. Reason: {e}")

    return num_of_rows_in_source


def check_table_difference(table):
    schema = table[0]
    table_name = table[1]
    order_field = table[2]

    include_schemas = ["ACCOUNT_PROCESSOR", "CREDIT_CARDS", "PUBLIC", "THREEDS_SVC", "THREEDS"]
    if schema not in include_schemas:
        return False

    if table_name in black_list:
        return False

    sf_count = get_sf_count(schema, table_name, order_field)
    pg_count = get_pg_count(schema, table_name, order_field)

    delta = pg_count - sf_count

    if pg_count != sf_count:
        print("delta:", delta, "count:", pg_count)
        slack_notifier.send_general_notification(
            message_text=f'Postgres Table: {schema}.{table_name}\nGap: {delta}')
        logging.info(f"Postgres Table: {schema}.{table_name}\nGap: {delta}")


def pg_sf_validation():
    slack_notifier.send_general_notification(
        message_text=f'Initiating Validation: {dag.dag_id}')
    logging.info(f"Initiating Validation: {dag.dag_id}")

    tables = get_sf_tables_list()

    for table in tables:
        check_table_difference(table)

    slack_notifier.send_general_notification(
        message_text=f'Finished Validation: {dag.dag_id}')
    logging.info(f"Finished Validation: {dag.dag_id}")


ARGS = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(2),
    "provide_context": True,
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    "on_failure_callback": partial(slack_notifier.send_notification, cnst.ERROR)

}

dag = DAG(
    dag_id="validation_dag_recent",
    default_args=ARGS,
    schedule="53 */4 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=int(VALIDATION_DAG_TIMEOUT))
)

with dag:
    pg_sf_validation_task = PythonOperator(
        task_id=f"pg_sf_validation",
        python_callable=pg_sf_validation,
    )
