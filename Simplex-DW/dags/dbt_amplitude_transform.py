from functools import partial
from datetime import timedelta
from airflow import utils
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from scripts.commons.aws.AwsManager import AwsManager
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.consts import Consts as cnst
from scripts.commons.dbt.DbtManager import DbtManager as dbt
from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager as PSM


env_config = Variable.get("env_config", deserialize_json=True)
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_MONITOR_PIPE)
channels = [cnst.AMPLITUDE_PIPE]

config = AwsManager.get_config(folder="transform_configs/",
                               file_name='amplitude_dbt_transforms',
                               file_type='yml')
extra_parameters = (Variable.get("dbt_time_delay", deserialize_json=True)).get('amplitude', '')

args = {
    "owner": "Airflow",
    "start_date": utils.dates.days_ago(2),
    "provide_context": True,
    "catchup": False,
    "max_active_runs": 1,
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    'on_failure_callback': partial(error_notifier.send_notification, cnst.ERROR, config=config, channels=channels,
                                   log_to_db=False),
}


@dag(
    dag_id=f"run_dbt_{config.get('transform_id')}",
    default_args=args,
    schedule=config.get('schedule_interval'),
    max_active_runs=1,
    catchup=False,
    tags=[cnst.DBT],
    dagrun_timeout=timedelta(minutes=30)
)
def run_transform():
    """ Pipeline to run dbt transformations in Snowflake DWH"""
    log_started = PythonOperator(
        task_id=f'log_started',
        python_callable=PSM.insert_2_etl_logs,
        op_kwargs={"status": 'log_started', "config_data": config})

    log_completed = PythonOperator(
        task_id=f'log_completed',
        python_callable=PSM.insert_2_etl_logs,
        op_kwargs={"status": 'log_completed', "config_data": config})

    last_run = None
    for model in config.get('models'):
        dbt_run_task = PythonOperator(
            task_id=f"run_model__{model.get('id')}",
            python_callable=dbt.dbt_run,
            op_kwargs={"dbt_command_select": f'''{model.get('select')} + {extra_parameters}'''})

        if not last_run:
            log_started >> dbt_run_task
        else:
            last_run >> dbt_run_task >> log_completed
        last_run = dbt_run_task

        if model.get('test'):
            dbt_test_task = PythonOperator(
                task_id=f"test_model__{model.get('id')}",
                python_callable=dbt.dbt_run,
                op_kwargs={"dbt_command_select": model.get('select'), "is_test": True})
            dbt_run_task >> dbt_test_task >> log_completed
            last_run = dbt_test_task


dag = run_transform()