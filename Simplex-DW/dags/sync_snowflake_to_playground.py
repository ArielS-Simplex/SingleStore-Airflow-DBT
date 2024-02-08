import airflow
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from scripts.commons.aws.AwsManager import AwsManager
from scripts.commons.snowflake_postgres.Snowflake2PostgresManager import Snowflake2PostgresManager as sf2pg
from datetime import timedelta

from functools import partial
from scripts.commons.consts import Consts as cnst
from scripts.commons.logger.SlackLogger import SlackLogger


# create logger/notifier
env_config = Variable.get("env_config", deserialize_json=True)
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_MONITOR_PIPE)


args = {
    "owner": "Airflow"
    , "start_date": airflow.utils.dates.days_ago(2)
    , "provide_context": True,
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    'on_failure_callback': partial(error_notifier.send_notification, cnst.ERROR)
}

@dag(
    dag_id="sync_snowflake_to_playground",
    default_args=args,
    schedule="08/10 * * * *",
    dagrun_timeout=timedelta(minutes=30),
    max_active_runs=1,
    catchup=False
)
def sync_snowflake_to_playground():
    """ Pipeline to sync datalake events data to Snowflake DWH"""
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    snow_conn = dwh_hook.get_conn()
    pg_hook = PostgresHook("postgres_playground")
    pg_conn = pg_hook.get_conn()
    config = AwsManager.get_config(folder="snowflake_to_postgres/",
                                   file_name='sync_snowflake_to_playground',
                                   file_type='yml')
    env_config = Variable.get("env_config", deserialize_json=True)
    infra = env_config['INFRA']
    config = config['offers_flatten'][infra]

    start = DummyOperator(task_id='start_events_sync')
    truncate_table = PostgresOperator(task_id=f'truncate_table',
                                      postgres_conn_id="postgres_playground",
                                      sql=f"""truncate bi_staging.snow_offers_flatten; """)
    load_staging = PythonOperator(task_id=f"load_staging", python_callable=sf2pg.snowflake_to_postgres,
                                  op_args=[pg_conn, pg_hook, snow_conn, config])
    copy_to_target_table = PostgresOperator(task_id=f'copy_to_target_table',
                                            postgres_conn_id="postgres_playground",
                                            sql=config['upsert'])

    start >> truncate_table >> load_staging >> copy_to_target_table


dag = sync_snowflake_to_playground()
