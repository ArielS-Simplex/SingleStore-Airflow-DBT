from scripts.crypto_pair_conversion_rate.transfer_data import transfer_data, delete_data
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from scripts.commons.consts import Consts as cnst
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager as PSM
from scripts.commons.aws.AwsManager import AwsManager
from datetime import datetime, timedelta
from airflow import DAG, utils
from functools import partial

# create logger/notifier
env_config = Variable.get("env_config", deserialize_json=True)
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_MONITOR_PIPE)

config = AwsManager.get_config(folder="cryptocurrency_pair_conversion_rate/",
                               file_name='config_cryptocurrency_pair_conversion_rate',
                               file_type='yml')

default_args = {
    'owner': 'data_engineer',
    'retries': 3,
    'on_failure_callback': partial(error_notifier.send_notification, cnst.ERROR, config=config, log_to_db=True)
}

with DAG(dag_id='cryptocurrency_pair_conversion_rate',
         start_date=utils.dates.days_ago(6),
         default_args=default_args,
         schedule_interval='00/10 * * * *',
         dagrun_timeout=timedelta(minutes=30),
         max_active_runs=1,
         catchup=True) as dag:
    log_started = PythonOperator(
        task_id=f'log_started',
        python_callable=PSM.insert_2_etl_logs,
        op_kwargs={"status": 'log_started', "config_data": config})

    delete_if_data_already_exists = PythonOperator(
        task_id='delete_if_data_already_exists',
        provide_context=True,
        python_callable=delete_data
    )

    insert_data_from_api_into_snowflake = PythonOperator(
        task_id='insert_data_from_api_into_snowflake',
        provide_context=True,
        python_callable=transfer_data
    )
    log_completed = PythonOperator(
        task_id=f'log_completed',
        python_callable=PSM.insert_2_etl_logs,
        op_kwargs={"status": 'log_completed', "config_data": config},
    )

    log_started >> delete_if_data_already_exists >> insert_data_from_api_into_snowflake >> log_completed
