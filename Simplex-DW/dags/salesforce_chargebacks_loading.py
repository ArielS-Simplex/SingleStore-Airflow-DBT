import logging as log
from airflow import DAG, utils
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from scripts.commons.consts import Consts as cnst
from scripts.commons.aws.AwsManager import AwsManager
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager
from scripts.commons.salesforce.SalesforceManager import SalesforceManager
from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager as PSM
import json
from datetime import timedelta
from functools import partial


env_config = Variable.get("env_config", deserialize_json=True)
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SALESFORCE)
config = AwsManager.get_config(folder="salesforce/", file_name="salesforce_chargebacks_loading", file_type="yml")

args = {
    "owner": "Airflow",
    "start_date": utils.dates.days_ago(2),
    "provide_context": True,
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    'on_failure_callback': partial(error_notifier.send_notification, cnst.ERROR, config=config, log_to_db=True),
}


@task()
def get_chargebacks_data(start: str):
    log.info('Getting chargebacks data from PG')
    pg_conn = config['source_conn_id']
    copy_sql = PostgreSQLManager.get_salesforce_query(query_name='CHARGEBACK_QUERY')
    pg_hook = PostgresHook.get_hook(pg_conn)
    df = pg_hook.get_pandas_df(sql=copy_sql)
    log.info('done run sql')
    log.info(len(df))
    log.info(df[0:10])
    chargebacks = json.loads(df.to_json(orient='records'))

    return chargebacks


@task()
def load_chargebacks_to_salesforce(chb_data):
    log.info('Loading chargebacks to Salesforce')
    try:
        log.info('Getting connection to Salesforce')
        sf = SalesforceManager.get_salesforce_client()
        log.info('Inserting chargeback records to Salesforce')
        sf.bulk.Chargeback__c.insert(chb_data, batch_size=1000, use_serial=True)
    except Exception as e:
        raise Exception(f"Could not load chargebacks data to Salesforce. Reason: {e}")


@task()
def log_started():
    context = get_current_context()
    return PSM.insert_2_etl_logs(status='log_started', config_data=config, **context)


@task()
def log_completed():
    context = get_current_context()
    return PSM.insert_2_etl_logs(status='log_completed', config_data=config, **context)


with DAG(
        dag_id="salesforce_chargebacks_loading",
        default_args=args,
        schedule="17 1,4,7,10,13,16,19,22 * * *",
        dagrun_timeout=timedelta(minutes=90),
        max_active_runs=1,
        catchup=False) as dag:
    log_started = log_started()
    chargebacks_data = get_chargebacks_data(log_started)
    load_chargebacks_to_salesforce = load_chargebacks_to_salesforce(chargebacks_data)

    chain(
        load_chargebacks_to_salesforce,
        log_completed()
    )




