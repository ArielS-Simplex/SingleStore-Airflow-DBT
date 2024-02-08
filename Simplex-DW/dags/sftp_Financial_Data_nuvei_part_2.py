from airflow import utils
from airflow.models import Variable
from airflow.decorators import dag
from airflow.decorators import task
from scripts.commons.aws.AwsManager import AwsManager
from scripts.commons.consts import Consts as cnst
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager as PSM
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from paramiko.ssh_exception import SSHException
from datetime import datetime, timedelta
from functools import partial
import logging
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
def s3_to_sftp_transfer(raw_config, sftp_conn, sftp_remote_path, s3_bucket, aws_conn_id='aws_default',**context):
    PSM.insert_2_etl_logs(status='log_started', config_data=raw_config, **context)
    task_instance = context['task_instance']
    for table_data in raw_config.get('models'):
        s3_key = task_instance.xcom_pull(dag_id='run_sftp_finance_part_1', task_ids='snowflake_to_s3',
                                         key=f'''sftp_dwh_{table_data['id']}''', include_prior_dates=True)
        logging.info('_____________')
        logging.info(s3_key)
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        csv_data = s3_hook.read_key(s3_key, bucket_name=s3_bucket)
        logging.info(csv_data[0:3])
        try:
            sftp_hook = SFTPHook(ssh_conn_id=sftp_conn)
            logging.info('____________0')
            with sftp_hook.get_conn() as sftp:
                # Change to the remote directory
                sftp.chdir('simplex')  # Test if remote_path exists
        finally:
            logging.info('________end_______')

    '''
            # Upload the CSV data to the SFTP server
            with sftp.file(self.s3_bucket_key, 'w') as remote_file:
                remote_file.write(csv_data)
           
            log.info("CSV data has been successfully transferred from S3 to the remote SFTP server.")

    except SSHException as e:
        log.error(f"Error testing or establishing the SFTP connection: {str(e)}")
    '''

@dag(
    dag_id=f"run_sftp_{config.get('transform_id')}_part_2",
    default_args=args,
    schedule=config.get('schedule_interval'),
    max_active_runs=3,
    catchup=False,
    tags=[cnst.DBT],
    dagrun_timeout=timedelta(minutes=30)

)
def run_sftp():
    """ Pipeline to run sftp in Snowflake DWH"""
    s3_to_sftp_transfer(raw_config=config, sftp_conn="sftp_nuvei", sftp_remote_path="simplex",
                        s3_bucket=env_config['s3_data_bucket'],aws_conn_id='aws_default')


dag = run_sftp()
