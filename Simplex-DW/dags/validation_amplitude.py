from airflow.decorators import dag, task
from airflow.models import Variable
from airflow import utils
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from functools import partial
from scripts.commons.consts import Consts as cnst
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.aws.AwsManager import AwsManager
from scripts.commons.snowflake.SnowflakeManager import SnowflakeManager as sfm
from datetime import timedelta

# get config for AWS
raw_config = AwsManager.get_config(folder="validation/",
                                   file_name='amplitude_validation',
                                   file_type='yml')
config = raw_config['config']

# create notifier
env_config = Variable.get("env_config", deserialize_json=True)
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_MONITOR_PIPE)
slack_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_EVENTS)
VALIDATION_DAG_TIMEOUT = Variable.get("validation_dag_timeout")


args = {
    "owner": "Airflow",
    "start_date": utils.dates.days_ago(2),
    "provide_context": True,
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    "on_failure_callback": partial(slack_notifier.send_notification, cnst.ERROR)

}


@dag(
    dag_id="validation_amplitude",
    default_args=args,
    schedule="41 */4 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=int(VALIDATION_DAG_TIMEOUT))
)
def run_dag():
    @task()
    def check_table_difference(tables):
        for c in tables:
            sfm.validation_row_count(config=tables[c], notifier=error_notifier)

    check_table_difference(tables=raw_config['tables'])


dag_instance = run_dag()
