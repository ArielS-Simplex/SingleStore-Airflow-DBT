from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.consts import Consts as cnst
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models import Variable
from functools import partial
from airflow import DAG
import logging
import airflow
import time

env_config = Variable.get("env_config", deserialize_json=True)
VALIDATION_DAG_TIMEOUT = Variable.get("validation_dag_timeout")
slack_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_EVENTS)
snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")


def get_sf_tables_list():
    try:
        table_list = snowflake_hook.get_records(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'DATA_LAKE'")
        # Extract table names from the tuples
        table_list = [table[0] for table in table_list]
        logging.info(f"Table list: {table_list}")
        return table_list
    except Exception as e:
        slack_notifier.send_general_notification(
            message_text='ERROR SF: Snowflake connection to retrieve tables list failed')
        logging.error(f"SF: failed to get tables. Reason: {e}")


def get_sf_timestamps(schema, table_name):
    timestamps = []  # Initialize timestamps as an empty list
    try:
        records = snowflake_hook.get_records(
            f"""SELECT DATA:time AS time 
            FROM SIMPLEX_RAW.{schema}.{table_name} 
            WHERE DATA:time BETWEEN DATEADD('day', -7, CURRENT_TIMESTAMP()) AND DATEADD('hour', -3, CURRENT_TIMESTAMP()) 
            ORDER BY time""")
        logging.info(f"Records for table {schema}.{table_name}: {records}")
        timestamps = [datetime.fromisoformat(record[0].strip('"').replace('Z', '+00:00')).replace(microsecond=0) for
                      record in records]

    except Exception as e:
        slack_notifier.send_general_notification(
            message_text=f'ERROR Snowflake: Failed to get timestamps for table {schema}.{table_name}')
        logging.error(f"Snowflake: Failed to get timestamps for table {schema}.{table_name}. Reason: {e}")
    return timestamps


# Define a dictionary mapping table names to gap thresholds (in seconds)
gap_thresholds = {
    'BUY_USER_INTERACTION_EVENTS': 15 * 60,  # 15 minutes
    'BUY_USER_EVENTS': 30 * 60,  # 30 minutes
    'COMPETITORS_COMPETITOR_QUOTES': 70 * 60,  # 70 minutes
    'PAYMENTS_PAYMENTS': 240 * 60  # 240 minutes
}
# The default gap threshold will be 5 minutes (300 seconds)
default_gap_threshold = 5 * 60


def check_gaps_in_table(table_name):
    schema = 'DATA_LAKE'
    logging.info(f"Checking gaps in table: {schema}.{table_name}")

    timestamps = get_sf_timestamps(schema, table_name)
    # Get the gap threshold for this table, or use the default if it's not defined
    gap_threshold = gap_thresholds.get(table_name, default_gap_threshold)
    gaps = [(timestamps[i - 1], timestamps[i], (timestamps[i] - timestamps[i - 1]).total_seconds())
            for i in range(1, len(timestamps))]

    if gaps:
        # Use the gap threshold for this table
        large_gaps = [gap for gap in gaps if gap[2] > gap_threshold]

        if large_gaps:
            # Group gaps by day
            gaps_by_day = {}
            for gap in large_gaps:
                gap_day = gap[0].strftime('%Y-%m-%d')
                if gap_day not in gaps_by_day:
                    gaps_by_day[gap_day] = []
                gaps_by_day[gap_day].append(gap)

            # Compile summary message
            summary_message = f'Summary for table: {schema}.{table_name}.\n'
            for day, day_gaps in gaps_by_day.items():
                total_gaps = len(day_gaps)
                total_gap_duration_seconds = (
                            day_gaps[-1][1] - day_gaps[0][0]).total_seconds()  # store total gap duration in seconds
                total_gap_duration = time.strftime('%H:%M:%S', time.gmtime(total_gap_duration_seconds))

                max_gap = max(day_gaps, key=lambda x: x[2])  # get the gap with the maximum duration
                first_gap_timestamp = day_gaps[0][0].strftime('%Y-%m-%dT%H:%M:%S')  # get the first gap timestamp
                last_gap_timestamp = day_gaps[-1][1].strftime('%Y-%m-%dT%H:%M:%S')  # get the last gap timestamp

                summary_message += (
                    f'{day} | Total gaps: {total_gaps} | '
                    f'Total gap duration: {total_gap_duration} | '  # use formatted total gap duration
                    f'Max gap: {str(timedelta(seconds=max_gap[2]))} | '
                    f'First gap: {first_gap_timestamp} | '  # add the first gap timestamp
                    f'Last gap: {last_gap_timestamp} |\n'  # add the last gap timestamp
                )
                logging.info(
                    f"Gaps for {day} in table: {schema}.{table_name}. Total gaps: {total_gaps}. Total gap duration: {total_gap_duration}. Max gap: {str(timedelta(seconds=max_gap[2]))}")

            # Send summary notification
            slack_notifier.send_general_notification(
                message_text=summary_message
            )


def check_gaps():
    slack_notifier.send_general_notification(
        message_text=f'Initiating gap check: {dag.dag_id}')
    logging.info(f"Initiating gap check: {dag.dag_id}")

    tables = get_sf_tables_list()

    for table_name in tables:
        check_gaps_in_table(table_name)

    slack_notifier.send_general_notification(
        message_text=f'Finished gap check: {dag.dag_id}')
    logging.info(f"Finished gap check: {dag.dag_id}")


ARGS = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(2),
    "provide_context": True,
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    "on_failure_callback": partial(slack_notifier.send_notification, cnst.ERROR)
}

dag = DAG(
    dag_id="validation_kafka",
    default_args=ARGS,
    schedule="17 */4 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=int(VALIDATION_DAG_TIMEOUT))
)
with dag:
    check_gaps_task = PythonOperator(
        task_id=f"check_gaps",
        python_callable=check_gaps,
    )
