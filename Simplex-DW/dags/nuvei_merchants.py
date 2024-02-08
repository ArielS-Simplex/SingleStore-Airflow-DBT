from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import re
import io

from functools import partial
from airflow.models import Variable
from scripts.commons.consts import Consts as cnst
from scripts.commons.logger.SlackLogger import SlackLogger

# create logger/notifier
env_config = Variable.get("env_config", deserialize_json=True)
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_MONITOR_PIPE)


def trim_timestamp(timestamp):
    regex = r'(.+)\.([0-9]{1,3})'

    matched = re.match(regex, timestamp)

    if matched is None:
        return timestamp

    datetime, millisecond = matched.groups()

    return f'{datetime}.{millisecond}'


def insert_df_into_postgres(conn, df, table):
    output = io.BytesIO()

    df.to_csv(output, sep='\t', header=True, index=False)

    output.seek(0)

    copy_query = f"COPY {table} FROM STDOUT csv DELIMITER '\t' NULL ''  ESCAPE '\\' HEADER "

    conn.cursor().copy_expert(copy_query, output)

    conn.commit()


file_suffix = 'GG_Trading.csv'
s3_prefix = 's3://splx-sftp-server-prod-nuvei/nuvei/'
db_schema = 'nuvei_merchants.nuvei_merchants_end_clients'

columns = {'TransactionDate': 'transaction_date',
           'Transactionmainid': 'transaction_main_id',
           'RequestID': 'request_id',
           'Amount': 'amount',
           'CurrencyCode': 'currency_code',
           'ClientName': 'client_name',
           'FullName': 'full_name',
           'Email': 'email'}


def transfer_data(**context):
    date = context['ds_nodash']

    key = f'{date}_{file_suffix}'

    s3_uri = f'{s3_prefix}{key}'

    df = pd.read_csv(s3_uri, encoding='UTF-16 LE')

    df = df.rename(columns=columns)

    df['transaction_date'] = df['transaction_date'].apply(trim_timestamp)

    postgres = PostgresHook(postgres_conn_id='postgres_nuvei_conn')

    insert_df_into_postgres(postgres.get_conn(), df, db_schema)


default_args = {
    'owner': 'data_engineer',
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    'on_failure_callback': partial(error_notifier.send_notification, cnst.ERROR)
}

with DAG(dag_id='nuvei_merchants',
         start_date=datetime(2022, 1, 18),
         schedule='0 0 * * 0-4',
         dagrun_timeout=timedelta(minutes=30),
         default_args=default_args,
         catchup=True) as dag:
    transfer_data_from_s3_to_postgres = PythonOperator(
        task_id='transfer_data_from_s3_to_postgres',
        python_callable=transfer_data
    )
