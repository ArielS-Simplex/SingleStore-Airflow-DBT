import logging as log
import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from scripts.commons.consts import Consts as cnst
from scripts.commons.aws.AwsManager import AwsManager
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.salesforce.SalesforceManager import SalesforceManager
from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager as PSM
import json
import pandas as pd
from datetime import timedelta
from functools import partial

POSTGRES_CONN_ID = 'postgres_playground'
env_config = Variable.get("env_config", deserialize_json=True)
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SALESFORCE)
config = AwsManager.get_config(folder="salesforce/", file_name="salesforce_chargebacks_update", file_type="yml")

query = config['query']
config = config['config']
args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(2),
    "provide_context": True,
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    'on_failure_callback': partial(error_notifier.send_notification, cnst.ERROR, config=config, log_to_db=True),
}


@task()
def get_salesforce_table_ids(start: str):
    log.info('get records from Salesforce')
    sf = SalesforceManager.get_salesforce_client()
    order_field = 'CreatedDate'
    max_result = 2000
    max_value = '1970-01-01T00:00:00Z'
    df = pd.DataFrame()

    while max_result >= 2000:
        chargebacks_ids = f'''SELECT Simplex_ID__c, Id, {order_field}
                                     FROM Chargeback__c
                                     WHERE  {order_field} >= {max_value}  AND {order_field} = LAST_N_DAYS:60
                                     order by {order_field} '''

        result = sf.query(chargebacks_ids)
        result = pd.DataFrame(result['records']).drop(['attributes'], axis=1)
        max_result = result.count()[0]
        df = pd.concat(objs=[df, result])
        max_value = df['CreatedDate'].max()
    keys_value = df.reset_index(drop=True).drop([order_field], axis=1)

    log.info(keys_value)
    keys_value = keys_value.values.tolist()

    return keys_value


@task()
def get_pg_data(sf_keys):
    log.info('transform sf_keys to dataframe')
    sf_keys = pd.DataFrame(sf_keys, columns=['Simplex_ID__c', 'Id'])
    sf_keys = sf_keys.astype({'Simplex_ID__c': 'int64', 'Id': 'object'})

    log.info('Getting chargebacks data from PG')
    try:
        log.info('Getting chargebacks data to pandas dataframe')
        pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
        df = pg_hook.get_pandas_df(sql=query)
        df = sf_keys.merge(df, how='inner', on='Simplex_ID__c')
        df = df.drop(['Simplex_ID__c'], axis=1)
        log.info('Transforming dataframe to json format')
        update_data = json.loads(df.to_json(orient='records'))
    except Exception as e:
        raise Exception(f"Could not get chargebacks data from {POSTGRES_CONN_ID}. Reason: {e}")

    return update_data


@task()
def update_chargebacks_in_salesforce(chb):
    try:
        log.info('Getting connection to Salesforce')
        sf = SalesforceManager.get_salesforce_client()
        log.info('updating chargeback records to Salesforce')
        sf.bulk.Chargeback__c.update(chb, batch_size=10000, use_serial=True)
    except Exception as e:
        raise Exception(f"Could not update chargebacks data to Salesforce. Reason: {e}")


@task()
def log_started():
    context = get_current_context()
    return PSM.insert_2_etl_logs(status='log_started', config_data=config, **context)


@task()
def log_completed():
    context = get_current_context()
    return PSM.insert_2_etl_logs(status='log_completed', config_data=config, **context)


with DAG(
        dag_id="salesforce_chargebacks_update",
        default_args=args,
        schedule="47 */3 * * *",
        dagrun_timeout=timedelta(minutes=30),
        max_active_runs=1,
        catchup=False) as dag:
    log_started = log_started()
    sf_table_ids = get_salesforce_table_ids(log_started)
    u_data = get_pg_data(sf_table_ids)
    update_chargebacks_in_salesforce = update_chargebacks_in_salesforce(u_data)

    chain(
        update_chargebacks_in_salesforce,
        log_completed()
    )

