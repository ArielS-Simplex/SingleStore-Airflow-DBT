import logging as log
from airflow import DAG, utils
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from scripts.commons.consts import Consts as cnst
from scripts.commons.aws.AwsManager import AwsManager
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager as PSM
from datetime import timedelta
from functools import partial


env_config = Variable.get("env_config", deserialize_json=True)
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SALESFORCE)
config = AwsManager.get_config(folder="other/", file_name="lp_status_to_slack", file_type="yml")

args = {
    "owner": "Airflow",
    "start_date": utils.dates.days_ago(2),
    "provide_context": True,
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    'on_failure_callback': partial(error_notifier.send_notification, cnst.ERROR, config=config, log_to_db=True),
}


@task()
def send_lp_status_notification(**context):
    PSM.insert_2_etl_logs(status='log_started', config_data=config, **context)

    # get data
    log.info('Getting lp data from PG')
    pg_conn = 'postgres_default'
    pg_hook = PostgresHook.get_hook(pg_conn)
    df = pg_hook.get_pandas_df(sql=config['sql'])
    log.info('done run sql')
    # create table plot
    df = df.round(0)
    column_names = df.columns.values[1:-1]
    df.update(df[column_names].applymap('{:,.0f}'.format))
    df['pred_left_percent'] = df['pred_left_percent'] / 100
    df.update(df[['pred_left_percent']].applymap('{:.0%}'.format))

    for v in config['versions']:
        df_filter = df
        config_filter = config['versions'][v]
        if config_filter['filter'] is True:
            df_filter = df.loc[df['lp'] == v]
            SlackLogger.plot_table_notification(df_filter=df_filter, grid_color=config_filter['grid_color'], channel_id=config_filter['channel_id'])
        else:
            SlackLogger.plot_table_notification(df_filter=df_filter, grid_color=config_filter['grid_color'], channel_id=config_filter['channel_id'])

    PSM.insert_2_etl_logs(status='log_completed', config_data=config, **context)


with DAG(
        dag_id="send_notification_lp_status",
        default_args=args,
        schedule="7/30 * * * *",
        dagrun_timeout=timedelta(minutes=90),
        max_active_runs=1,
        catchup=False) as dag:
    lp_status_data = send_lp_status_notification()
