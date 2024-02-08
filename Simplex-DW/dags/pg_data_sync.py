import logging
from datetime import datetime, timedelta
from functools import partial
import airflow
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.utils.timezone import make_aware
from scripts.operators.postgres_to_s3_operator import PostgresToS3Operator
from scripts.operators.s3_to_snowflake_operator import S3ToSnowflakeOperator
from airflow.models import Variable
from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager as PSM
from scripts.commons.consts import Consts as cnst
from scripts.commons.logger.SlackLogger import SlackLogger
from scripts.commons.aws.AwsManager import AwsManager
from scripts.commons.snowflake.SnowflakeManager import SnowflakeManager as SFM
from airflow.decorators import dag, task

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

env_config = Variable.get("env_config", deserialize_json=True)
config = AwsManager.get_config(folder="pg_data_sync/", file_name="config", file_type="json")
config.update(config.get("rules")[0])
del config['rules'] # remove duplicate data
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_MONITOR_PIPE)
timeout = config.get("timeout", env_config["default_timeout"])
s3_data_bucket = config["s3_data_bucket"]
if env_config['INFRA'] != cnst.PROD_ENV:
    s3_data_bucket = cnst.STAGE_DATA_SYNC_BUCKET
tags = ['datalake', config["schema_name"], config["s3_data_prefix"]]
if "tags" in config:
    tags = tags + config["tags"]


args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(2),
    'on_failure_callback': partial(error_notifier.send_notification, cnst.ERROR, config=config, log_to_db=True),
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
}


@dag(
    dag_id="pg_data_sync", default_args=args, schedule=config["schedule"],
    dagrun_timeout=timedelta(minutes=timeout), max_active_runs=1,
    catchup=False, start_date=airflow.utils.dates.days_ago(2),
    tags=tags
)
def build_dag_from_config():
    config_data = config
    disable = config_data.get("disable", "")
    s3_data_prefix = config_data["s3_data_prefix"]
    source_conn_id = config_data["source_conn_id"]
    target_conn_id = config_data["target_conn_id"]
    target_stage = config_data["target_stage"]
    target_role = config_data["target_role"]
    batch_max_rows = config_data["batch_max_rows"]
    file_format = config_data["file_format"]
    file_type = config_data.get("file_type", "csv")
    backward_update_period = config_data.get("backward_update_period", None)
    batch_min_rows = config_data.get("batch_min_rows", 0)
    prefix = config_data.get("prefix", "")
    suffix = config_data.get("suffix", "")
    source_table_name = config_data["table_name"]
    source_schema_name = config_data["schema_name"]
    target_table_name = config_data.get("target_table_name", source_table_name)
    table_name = f"{prefix}{target_table_name}{suffix}"
    schema_name = config_data.get("target_schema_name", source_schema_name)
    stage_schema = schema_name
    stage_table = table_name
    load_method = config_data.get("load_method", "insert-only")
    order_field = config_data.get("order_field", None)
    key_field = config_data.get("key_field", None)
    volume = config_data.get("volume", "medium")
    columns_to_hash = config_data.get("columns_to_hash", None)
    sql = config_data.get("sql", f"select * from {source_schema_name}.{source_table_name}")
    sql_condition = config_data.get("sql_condition", "")



    def get_target_table_info(**context):
        PSM.insert_2_etl_logs(status='log_started', config_data=config_data,s3_keys=None,**context)
        task_instance = context['task_instance']
        dd = datetime.now()
        path = dd.strftime("%Y/%m/%d/%H/")
        filename = dd.strftime("%Y%m%d_%H%M%S_%f")
        full_path = f"{path}{source_conn_id}_{schema_name}_{table_name}_{filename}.{file_type}"
        task_instance.xcom_push(key=f"{schema_name}.{table_name}.filename", value=full_path)
        max_order_field = SFM.get_max_order_field(schema_name, table_name, order_field, backward_update_period)
        columns_list = SFM.columns_list_query(schema_name, table_name)
        logging.info(f"latest id: {max_order_field[0]}, table count : {max_order_field[1]}")
        merge_query = SFM.merge_query(load_method, schema_name, target_table_name, key_field)
        task_instance.xcom_push(key=f"{schema_name}_{table_name}.merge_query", value=merge_query)
        task_instance.xcom_push(key=f"{schema_name}_{table_name}.max_order_id", value=max_order_field[0])
        task_instance.xcom_push(key=f"{schema_name}_{table_name}.columns_list", value=columns_list)

    if not disable:
        get_target_table_info_task = PythonOperator(
                task_id=f"start___get_target_table_info",
                python_callable=get_target_table_info,
                provide_context=True,
            )

        max_order_id = f"'{{{{task_instance.xcom_pull(key='{schema_name}_{table_name}.max_order_id')}}}}'"
        # two case of load methods are handled , merge and truncate-insert. default is insert only and data is
        if load_method in ["merge", "insert-only"]:
            stage_schema = cnst.STAGE_ENV
            stage_table = f"{schema_name}_{table_name}"

            # create a stage table for loading this batch before merging with target table
            create_stage_table = SnowflakeOperator(
                task_id=f'create_stage_table',
                snowflake_conn_id=target_conn_id,
                sql=f"CREATE OR REPLACE TABLE {stage_schema}.{stage_table} LIKE {schema_name}.{table_name}",
                schema=schema_name,
                role=target_role,
            )

            # after loading data to stage run the merge command
            merge_task = SnowflakeOperator(
                task_id=f'merge',
                snowflake_conn_id=target_conn_id,
                sql=f"""{{{{ task_instance.xcom_pull(key="{schema_name}_{table_name}.merge_query") }}}}""",
                schema=schema_name,
                role=target_role,
            )
        elif load_method == "truncate-insert":
            truncate_table = SnowflakeOperator(
                task_id=f'truncate_table',
                snowflake_conn_id=target_conn_id,
                sql=f"TRUNCATE TABLE {table_name}",
                schema=schema_name,
                role=target_role,
            )
        else:
            raise ValueError('load_method is not recognized')

        method = cnst.no_columns_to_hash
        if columns_to_hash is not None:
            method = cnst.columns_to_hash

        # data can be extract incrementally only if order_field is available
        pg_extract_query = PSM.pg_extract_query(source_schema_name, source_table_name, sql, sql_condition,
                                                batch_max_rows, max_order_id, order_field, volume)

        extract_from_pg_load_to_s3_task = PostgresToS3Operator(
            task_id=f"extract_from_pg_load_to_s3",
            postgres_conn_id=source_conn_id,
            query=pg_extract_query,
            aws_conn_id="aws_conn",
            s3_bucket=s3_data_bucket,
            s3_key=f"""{s3_data_prefix}/{schema_name}/{table_name}/{{{{ task_instance.xcom_pull(key="{schema_name}.{table_name}.filename") }}}}""",
            batch_min_rows=batch_min_rows,
            method=method,
            columns_to_hash=columns_to_hash,
        )
        # only if load method is merge data will be loaded first to the stage schema and not to target schema
        copy_into_table_task = S3ToSnowflakeOperator(
            task_id=f'copy_into_table',
            snowflake_conn_id=target_conn_id,
            s3_keys=[
                f"""{schema_name}/{table_name}/{{{{ task_instance.xcom_pull(key="{schema_name}.{table_name}.filename") }}}}"""],
            table=stage_table,
            schema=stage_schema,
            stage=target_stage,
            role=target_role,
            columns_array=f"""{{{{ task_instance.xcom_pull(key="{schema_name}_{table_name}.columns_list") }}}}""".split(","),
            file_format=f"(format_name={file_format})",
        )

        log_completed = PythonOperator(
            task_id=f'log_completed',
            python_callable=PSM.insert_2_etl_logs,
            op_kwargs={"status": 'log_completed', "config_data": config,
                       "s3_keys": f"""{schema_name}/{table_name}/{{{{ task_instance.xcom_pull(key="{schema_name}.{table_name}.filename") }}}}"""},
        )

        if load_method in ["merge", "insert-only"]:
            # merge or insert-only
            get_target_table_info_task >> extract_from_pg_load_to_s3_task >> create_stage_table >> copy_into_table_task >> merge_task >> log_completed

        elif load_method == "truncate-insert":
            # truncate-insert
            get_target_table_info_task >> extract_from_pg_load_to_s3_task >> truncate_table >> copy_into_table_task >> log_completed
        else:
            raise ValueError('load_method is not recognized')


dag = build_dag_from_config()
