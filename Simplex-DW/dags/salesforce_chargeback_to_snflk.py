import logging
import logging as log
import airflow
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from scripts.commons.consts import Consts as cnst
from scripts.commons.aws.AwsManager import AwsManager
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.salesforce.queries import sf_queries
from scripts.commons.salesforce.SalesforceManager import SalesforceManager
from scripts.commons.snowflake.SnowflakeManager import SnowflakeManager
from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager as PSM
import pandas as pd
from datetime import timedelta, datetime
from functools import partial

env_config = Variable.get("env_config", deserialize_json=True)
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SALESFORCE)
config = AwsManager.get_config(folder="salesforce_to_snowflake/",
                               file_name='salesforce_chargebacks_to_snowflake',
                               file_type='yml')
args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(2),
    "provide_context": True,
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    'on_failure_callback': partial(error_notifier.send_notification, cnst.ERROR, config=config, log_to_db=True),
}

@dag(
    dag_id="salesforce_chargebacks_to_snflk",
    default_args=args,
    schedule="00 22 * * * ",
    dagrun_timeout=timedelta(minutes=40),
    max_active_runs=1,
    catchup=False
)
def salesforce_chargebacks_to_snowflake():

    @task()
    def load_salesforce_chargebacks_data():
        log.info('get chargebacks from Salesforce')
        try:
            log.info('Getting connection to Salesforce')
            sf = SalesforceManager.get_salesforce_client()
            log.info('get generator for Salesforce query')
            latest_update = SnowflakeManager.get_latest_chargeback_update_time()
            # parse latest_update - epoch to timestamp without milliseconds
            latest_update = datetime.fromtimestamp(int(str(latest_update)[:-3])).strftime('%Y-%m-%dT%H:%M:%SZ')
            log.info(f'latest update - {latest_update}')
            log.info(f"running query - {sf_queries.chargebacks_log}{latest_update}")
            chargebacks_data_gen = sf.bulk.Chargeback__c.query(sf_queries.chargebacks_log+latest_update, lazy_operation=True)
            for data in chargebacks_data_gen:
                log.info(f'Creating bulk insert task')
                batch = pd.DataFrame.from_dict(data, orient='columns').drop('attributes', axis=1)
                SnowflakeManager.upsert_chargebacks_to_snowflake(batch)
        except Exception as e:
            raise Exception(f"Could not get chargebacks generator from Salesforce. Reason: {e}")
    @task()
    def log_started(config_data, **context):
        PSM.insert_2_etl_logs(status='log_started', config_data=config_data, **context)
        return

    @task()
    def log_completed(config_data, **context):
        return PSM.insert_2_etl_logs(status='log_completed', config_data=config_data, **context)

    t_log_started = log_started(config)
    t_load_salesforce_chargebacks_data = load_salesforce_chargebacks_data()
    t_log_completed = log_completed(config)

    t_log_started >> t_load_salesforce_chargebacks_data >> t_log_completed


dag = salesforce_chargebacks_to_snowflake()



