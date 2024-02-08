from airflow import utils
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from scripts.commons.consts import Consts as cnst
from scripts.commons.aws.AwsManager import AwsManager
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.snowflake.SnowflakeManager import SnowflakeManager as SFM
from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager as PSM
from datetime import timedelta, date
from functools import partial

env_config = Variable.get("env_config", deserialize_json=True)
target_database = env_config.get('database_name')
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_MONITOR_PIPE)
config = AwsManager.get_config(folder="snowflake_data_ingestion/",
                               file_name='copy_s3_into_snowflake',
                               file_type='yml')

args = {
    "owner": "Airflow",
    "start_date": utils.dates.days_ago(2),
    "provide_context": True,
    "catchup": False,
    "max_active_runs": 1,
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    'on_failure_callback': partial(error_notifier.send_notification, cnst.ERROR, config=config, log_to_db=True),
}


@task()
def copy_s3_events_to_dwh(conf_value, **context):

    stage_schema = conf_value['stage_schema']
    stage_name = conf_value['stage_name']
    target_table = conf_value['target_table']
    target_schema = conf_value['target_schema']
    s3_source_pattern = conf_value['s3_source_pattern']
    file_format = conf_value['file_format']
    raw_s3_path = conf_value['s3_path']
    s3_events = Variable.get("s3_events", deserialize_json=True)

    yesterday = (date.today() - timedelta(days=1)).strftime(s3_events['data_format'])
    yesterday_s3_path = f'{raw_s3_path}{yesterday}'
    today = date.today().strftime(s3_events['data_format'])
    today_s3_path = f'{raw_s3_path}{today}'
    SFM.execute_snowflake_query(
        target= f'{target_database}.{target_schema}.{target_table}',
        stage= f'{target_database}.{stage_schema}.{stage_name}',
        pattern=s3_source_pattern,file_format=file_format,s3_path=yesterday_s3_path)
    SFM.execute_snowflake_query(
        target= f'{target_database}.{target_schema}.{target_table}',
        stage= f'{target_database}.{stage_schema}.{stage_name}',
        pattern=s3_source_pattern,file_format=file_format,s3_path=today_s3_path)

    SFM.de_duplication(target_schema=target_schema,
                       target_table=target_table)


@dag(
    dag_id="sync_events_to_dwh",
    default_args=args,
    schedule='06/10 * * * *',
    max_active_runs=1,
    catchup=False,
    tags=[cnst.EVENTS_TAG],
    dagrun_timeout=timedelta(minutes=30)
)
def sync_events_to_dwh():
    """ Pipeline to sync datalake events data to Snowflake DWH """
    log_started = PythonOperator(
        task_id=f'log_started',
        python_callable=PSM.insert_2_etl_logs,
        op_kwargs={"status": 'log_started', "config_data": config})

    log_completed = PythonOperator(
        task_id=f'log_completed',
        python_callable=PSM.insert_2_etl_logs,
        op_kwargs={"status": 'log_completed', "config_data": config})

    with TaskGroup(group_id='sync_events_to_dwh_group') as sync_events_to_dwh_group:  # Define your TaskGroup
        for conf_name, conf_value in config['events'].items():
            copy_s3_events_to_dwh.override(task_id=f'copy_s3_events_to_dwh_{conf_name}')(conf_value)

    log_started >> sync_events_to_dwh_group >> log_completed


dag = sync_events_to_dwh()
