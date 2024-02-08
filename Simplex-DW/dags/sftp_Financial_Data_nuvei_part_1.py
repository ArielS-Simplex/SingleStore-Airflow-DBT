from airflow import utils
from airflow.models import Variable
from airflow.decorators import dag
from airflow.decorators import task
from scripts.commons.aws.AwsManager import AwsManager
from scripts.commons.consts import Consts as cnst
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager as PSM
from datetime import datetime, timedelta
from functools import partial
import logging
import pandas as pd
from tabulate import tabulate
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

env_config = Variable.get("env_config", deserialize_json=True)
config = AwsManager.get_config(folder="sftp/", file_name="finance_to_nuvei_dwh", file_type="yml")
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_MONITOR_PIPE)

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
def snowflake_to_s3(raw_config, aws_conn_id='aws_conn_id', **context):
    PSM.insert_2_etl_logs(status='log_started', config_data=raw_config, **context)
    for table_data in raw_config.get('models'):
        dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        snow_conn = dwh_hook.get_conn()
        logging.info(table_data['query'])
        df = pd.read_sql(sql=table_data['query'], con=snow_conn)
        logging.info(tabulate(df, headers='keys', tablefmt='psql'))
        logging.info("Data from snowflake obtained")

        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        s3_key =f"""{raw_config['s3_data_prefix']}/{datetime.now().strftime("%Y/%m")}/{table_data['id']}{datetime.now().strftime("%Y%m%d%H%M%S")}.{raw_config["file_type"]}"""
        logging.info(s3_key)
        s3_hook.load_string(
            string_data=df.to_csv(index=False),
            key=s3_key,
            bucket_name=env_config['s3_data_bucket'],
            replace=True)
        logging.info("Data was upload to s3")
        task_instance = context['task_instance']
        task_instance.xcom_push(key=f'''sftp_dwh_{table_data['id']}''', value=s3_key)
        csv_file = io.BytesIO(df.to_csv(index=False).encode('utf-8'))

        SlackLogger.send_notification_file(file=csv_file, filename=f'''{table_data['id']}.csv''',
                                           channel_id=raw_config['channel_id'])

    PSM.insert_2_etl_logs(status='log_completed', config_data=raw_config, **context)


@dag(
    dag_id=f"run_sftp_{config.get('transform_id')}_part_1",
    default_args=args,
    schedule=config.get('schedule_interval'),
    max_active_runs=3,
    catchup=False,
    tags=[cnst.DBT],
    dagrun_timeout=timedelta(minutes=30)
)

def run_sftp():
    """ Pipeline to run sftp in Snowflake DWH"""
    snowflake_to_s3(raw_config=config, aws_conn_id= "aws_conn")


dag = run_sftp()
