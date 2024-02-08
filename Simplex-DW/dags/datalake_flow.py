import json
import logging
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.timezone import make_aware
from scripts.operators.postgres_to_s3_operator import PostgresToS3Operator
from scripts.operators.s3_to_snowflake_operator import S3ToSnowflakeOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


SLACK_CONN_ID = 'slack'
DATALAKE_FLOW_TIMEOUT = Variable.get("datalake_flow_timeout")


#[TODO]: refactor to  commons->logger->SlackLogger
def task_fail_slack_alert(context):

    exception = str(context['exception']).replace("'", "")

    if exception == "Detected as zombie":
        return

    log_url = context.get('task_instance').log_url
    dag_id = context.get('task_instance').dag_id

    try:
        task_id = str(context.get('task_instance').task_id)

        if "log_started" not in task_id:

            schema_and_table = dag_id.replace("datalake_flow_", "")

            data_utils_db_hook = PostgresHook(postgres_conn_id="data-utils-db")
            data_utils_db_conn = data_utils_db_hook.get_conn()

            sql_query = f"""
                    INSERT INTO public.etl_logs(connection_id,schema_name,table_name,dag_name,filename_key,event_ts,status,error_message)
                    select connection_id,schema_name,table_name,dag_name,filename_key,CURRENT_TIMESTAMP,'FAILED','{exception}\n{log_url}'
                    from (
                          select row_number() over(partition by schema_name,table_name order by event_ts desc) row_num,*
                          from public.etl_logs
                          where schema_name || '_' || table_name ='{schema_and_table}'
                                and status='STARTED'
                        ) etl_logs
                    where row_num = 1;
                    """

            cursor = data_utils_db_conn.cursor()
            cursor.execute(sql_query)
            data_utils_db_conn.commit()

            slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
            slack_msg = """
                        :red_circle: Task Failed.
                        *Task*: {task}
                        *Dag*: {dag}
                        *Execution Time*: {exec_date}
                        *Exception*: {exception}
                        *Log Url*: {log_url}
                        """.format(
                task=context.get('task_instance').task_id,
                dag=context.get('task_instance').dag_id,
                ti=context.get('task_instance'),
                exec_date=context.get('execution_date'),
                exception=exception,
                log_url=context.get('task_instance').log_url,
            )
            failed_alert = SlackWebhookOperator(
                task_id='slack_test',
                http_conn_id='slack',
                webhook_token=slack_webhook_token,
                message=slack_msg,
                username='airflow')

            return failed_alert.execute(context=context)

    except Exception as e:
        print(e)
        pass


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(2),
    "provide_context": True,
    'on_failure_callback': task_fail_slack_alert #[TODO] SlackLogger.send_notification(notification_type = ERROR, log_to_db = True)
}


#[TODO]: not in use. delete
def need_to_fill_the_gap(context):
    table_name = context["table_name"]
    schema_name = context["schema_name"]
    source_conn_id = context["source_conn_id"]
    sql = context["sql"]
    order_field = context["order_field"]
    batch_max_rows = int(context["batch_max_rows"])
    source_vs_target = 1000000000

    try:
        dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

        sql_query = f"SELECT CURRENT_TIMESTAMP,COUNT(*) FROM {schema_name}.{table_name}"

        if order_field is not None:
            sql_query = f"SELECT COALESCE(MAX({order_field}),'0'),COUNT(*) FROM {schema_name}.{table_name}"

        result = dwh_hook.get_first(sql_query)

        latest_id = str(result[0])
        table_count = int(result[1])

        if table_count > 0:
            postgres_hook = PostgresHook(postgres_conn_id=source_conn_id)
            cursor = postgres_hook.get_conn().cursor()
            cursor.execute(f"""
                                SELECT count(*)
                                FROM ({sql}) q
                                WHERE {order_field} > '{latest_id}'
                                """.rstrip())
            source_vs_target = int(cursor.fetchone()[0])
            logging.info(f"source_vs_target = {source_vs_target}")

    except Exception as e:
        print(e)
        pass

    if source_vs_target > batch_max_rows:
        return True

    return False

#[TODO]: split into get_table_info() and generate_merge_query()
#Both get_table_info and generate_merge_query should be in SnowflakeManager
#optional - switch xcom to taskflow
def get_target_table_info(**context):
    table_name = context["table_name"]
    schema_name = context["schema_name"]
    order_field = context["order_field"]
    source_conn_id = context["source_conn_id"]
    file_type = context["file_type"]
    key_field = str(context["key_field"]).lower()
    task_instance = context['task_instance']
    timeout = context["timeout"]
    load_method = context["load_method"]
    sql = context["sql"]
    batch_max_rows = int(context["batch_max_rows"])
    backward_update_period = None
    latest_id = 0
    table_count = 0
    columns_list = ""
    merge_command = ""

    try:
        backward_update_period = context["backward_update_period"]
    except Exception as e:
        pass

    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    sql_query = f"SELECT CURRENT_TIMESTAMP,COUNT(*) FROM {schema_name}.{table_name}"

    if order_field is not None:
        sql_query = f"SELECT COALESCE(MAX({order_field}),'0'),COUNT(*) FROM {schema_name}.{table_name}"

        if backward_update_period is not None and order_field.lower() in ['created_at', 'inserted_at', 'updated_at', 'valid_until']:
            sql_query = f"""
                            select 
                                case when current_timestamp - interval '{backward_update_period}' > max_date then 
                                    max_date 
                                else 
                                    current_timestamp - interval '{backward_update_period}' 
                                end latest_id,
                                table_count
                            from (
                                SELECT COALESCE(MAX({order_field}),'0') max_date ,COUNT(*) table_count
                                FROM {schema_name}.{table_name}
                            );"""

    result = dwh_hook.get_first(sql_query)

    latest_id = str(result[0]) #[TODO] give better name
    table_count = str(result[1])

    logging.info(f"latest id: {latest_id}")
    logging.info(f"table count : {table_count}")

    sql_query = f"""select listagg(column_name , ', ') within group (ORDER BY ordinal_position)
                    from information_schema.columns
                    where lower(table_name)='{table_name}' 
                          and lower(table_schema)='{schema_name}'
                          and column_default is null"""

    result = dwh_hook.get_first(sql_query)
    columns_list = str(result[0])

    #[TODO]: end of get_target_table_info
    #start of generate_merge_query()

    if key_field is not None:
        if load_method == "merge":
            sql_query = f"""
                            select  'merge into ' || table_name || ' AS target using stage.' || table_schema || '_' || table_name || ' AS source on target.{key_field} = source.{key_field}\r\n' ||
                                    'when matched then update set ' || listagg('target.' || column_name || '=source.' || column_name, ', ') within group (ORDER BY ordinal_position) || '\r\n' ||
                                    'when not matched then insert ({key_field},' || listagg(column_name, ', ') within group (ORDER BY ordinal_position) || ') values (source.{key_field},' || listagg('source.' || column_name, ', ') within group (ORDER BY ordinal_position) || ')'
                            from information_schema.columns
                            where lower(table_name)='{table_name}' and lower(table_schema)='{schema_name}' 
                                and lower(column_name) not in ('{key_field}')
                                and column_default is null
                            group by table_name,table_schema
                            """
        elif load_method == "insert-only":
            sql_query = f"""
                            select  'merge into ' || table_name || ' AS target using stage.' || table_schema || '_' || table_name || ' AS source on target.{key_field} = source.{key_field}\r\n' ||
                                    'when not matched then insert ({key_field},' || listagg(column_name, ', ') within group (ORDER BY ordinal_position) || ') values (source.{key_field},' || listagg('source.' || column_name, ', ') within group (ORDER BY ordinal_position) || ')'
                            from information_schema.columns
                            where lower(table_name)='{table_name}' and lower(table_schema)='{schema_name}' 
                                and lower(column_name) not in ('{key_field}')
                                and column_default is null
                            group by table_name,table_schema
                            """

        result = dwh_hook.get_first(sql_query)
        merge_command = str(result[0])
        #end of generate_merge_query()
        task_instance.xcom_push(key=f"{schema_name}.{table_name}.merge_command", value=merge_command)

    task_instance.xcom_push(key=f"{schema_name}.{table_name}", value=latest_id)
    task_instance.xcom_push(key=f"{schema_name}.{table_name}.count", value=table_count)
    task_instance.xcom_push(key=f"{schema_name}.{table_name}.columns_list", value=columns_list)

    #[TODO]: refactor - use datetime.now() as default value when result[0] is None
    dd = datetime.now()
    try:
        dd = datetime.datetime.fromisoformat(str(result[0]))
    except Exception as e:
        pass

    path = dd.strftime("%Y/%m/%d/%H/")
    filename = dd.strftime("%Y%m%d_%H%M%S_%f")
    full_path = f"{path}{source_conn_id}_{schema_name}_{table_name}_{filename}.{file_type}"
    task_instance.xcom_push(key=f"{schema_name}.{table_name}.filename", value=full_path)

#[TODO]: deprecated. use get_config from AWSManager instead
def get_config(config, config_name):

    logging.info("loading config from s3")
    s3_conn = S3Hook(aws_conn_id="aws_conn", verify=True)

    config_from_s3 = s3_conn.get_key(key=f"""{config["configs_s3_path"]}{config_name}.json""", bucket_name=config["configs_s3_bucket"])
    file_content = config_from_s3.get()['Body'].read().decode('utf-8')
    json_content = json.loads(str(file_content))
    logging.info("Finished loading")

    return json_content

#[TODO]: Use taskflow
#https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
# or offers/amplitude dag as reference
def build_dag_from_config(config):
    schedule_interval = config["schedule"]
    s3_data_prefix = config["s3_data_prefix"]
    s3_data_bucket = config["s3_data_bucket"]
    source_conn_id = config["source_conn_id"]
    target_conn_id = config["target_conn_id"]
    target_stage = config["target_stage"]
    target_role = config["target_role"]
    batch_max_rows = config["batch_max_rows"]
    batch_min_rows = 0
    file_format = config["file_format"]
    prefix = ""
    suffix = ""
    file_type = "csv"
    timeout = env_config["default_timeout"]

    #[TODO]: use get and default value
    try:
        batch_min_rows = config["batch_min_rows"]
    except Exception as e:
        pass

    try:
        prefix = config["prefix"]
    except Exception as e:
        pass

    try:
        suffix = config["suffix"]
    except Exception as e:
        pass

    try:
        file_type = config["file_type"]
    except Exception as e:
        pass

    try:
        timeout = config["timeout"]
    except Exception as e:
        pass

    source = config
    try:
        source.update(config["rules"][0])
    except Exception as e:
        pass

    source_table_name = source["table_name"]
    table_name = f"{prefix}{source_table_name}{suffix}"
    source["table_name"] = table_name
    schema_name = source["schema_name"]
    source["target_conn_id"] = target_conn_id
    load_method = "insert-only"
    order_field = None
    key_field = None
    volume = "medium"
    columns_to_hash = None

    source["file_type"] = file_type
    source["source_conn_id"] = source_conn_id
    source["s3_data_prefix"] = s3_data_prefix
    source["target_role"] = target_role
    source["timeout"] = timeout
    stage_schema = schema_name
    stage_table = table_name
    sql = f"select * from {schema_name}.{source_table_name}"
    sql_condition = ""
    disable = False

    try:
        sql = source["sql"]
    except Exception as e:
        pass

    try:
        sql_condition = source["sql_condition"]
    except Exception as e:
        pass

    try:
        order_field = source["order_field"]
    except Exception as e:
        pass

    try:
        disable = source["disable"]
    except Exception as e:
        pass

    try:
        load_method = source["load_method"]
    except Exception as e:
        pass

    try:
        volume = source["volume"]
    except Exception as e:
        pass

    try:
        key_field = source["key_field"]
    except Exception as e:
        source["key_field"] = None
        if load_method == "merge":
            raise
        else:
            pass

    source["order_field"] = order_field

    try:
        columns_to_hash = source["columns_to_hash"]
    except Exception as e:
        pass
    #[TODO]: not in use - delete
    '''
    if load_method in ["merge", "insert-only"]:
        if need_to_fill_the_gap(source):
            schedule_interval = "* * * * *"
    '''
    tags = ['datalake', source["schema_name"], source["s3_data_prefix"]]
    if "tags" in source:
        tags = tags + source["tags"]

    dag = DAG(
        dag_id="datalake_flow", default_args=args, schedule_interval=schedule_interval,
        dagrun_timeout=timedelta(minutes=int(DATALAKE_FLOW_TIMEOUT)),
        max_active_runs=1,
        catchup=False, start_date=airflow.utils.dates.days_ago(2),
        tags=tags
    )

    with dag:

        dag_name = dag.dag_id
        source["dag_name"] = dag_name

        if not disable:

            # two case of load methods are handled , merge and truncate-insert. default is insert only and data is
            # loaded directly to target table
            if load_method in ["merge", "insert-only"]:
                # only if load method is merge data will be loaded first to the stage schema and not to target schema
                stage_schema = "stage"
                stage_table = f"{schema_name}_{table_name}"

                # create a stage table for loading this batch before merging with target table
                create_stage_table = SnowflakeOperator(
                    task_id=f'create_stage_table',
                    snowflake_conn_id=target_conn_id,
                    sql=f"CREATE OR REPLACE TABLE {stage_schema}.{stage_table} LIKE {table_name}",
                    schema=schema_name,
                    role=target_role,
                )

                # after loading data to stage run the merge command
                merge_task = SnowflakeOperator(
                    task_id=f'merge',
                    snowflake_conn_id=target_conn_id,
                    sql=f"""{{{{ task_instance.xcom_pull(key="{schema_name}.{table_name}.merge_command") }}}}""",
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

            #[TODO]: runs first, put before snowflake operators
            get_target_table_info_task = PythonOperator(
                task_id=f"get_target_table_info",
                python_callable=get_target_table_info,
                op_kwargs=source
            )

            method = 2
            if columns_to_hash is not None:
                method = 1

            #[TODO]: SQL code should be in PostgresSQLManager and SnoflakeManager
            # data can be extract incrementally only if order_field is available
            if order_field is not None:
                extra_where_high_volume = ""
                if volume.lower() == "high":
                    if order_field.lower() in ['created_at', 'inserted_at', 'updated_at', 'valid_until']:
                        extra_where_high_volume = f'''
                                              WHERE  {order_field} >'{{{{ task_instance.xcom_pull(key="{schema_name}.{table_name}") }}}}' {sql_condition}
                                                AND {order_field} < date_trunc('minute',(select max({order_field}) AS max_date from(
                                                                                                                select {order_field}
                                                                                                                FROM ({sql}) as {schema_name}_{table_name}
                                                                                                                WHERE {order_field} >'{{{{ task_instance.xcom_pull(key="{schema_name}.{table_name}") }}}}'
                                                                                                                order by {order_field} asc
                                                                                                                limit {batch_max_rows}) as m)) - interval '1 minutes'                         
                                                '''
                    else:
                        extra_where_high_volume = f"""WHERE {order_field} <= (SELECT max({order_field}) FROM {schema_name}.{table_name} """

                inc_extract_from_pg_load_to_s3_task = PostgresToS3Operator(
                    task_id=f"extract_from_pg_load_to_s3",
                    postgres_conn_id=source_conn_id,
                    query=f"""
                            WITH all_data AS
                            (
                                SELECT *
                                FROM ({sql} {extra_where_high_volume}) q
                                WHERE {order_field} > '{{{{ task_instance.xcom_pull(key="{schema_name}.{table_name}") }}}}'
                                ORDER BY {order_field} ASC
                                LIMIT {batch_max_rows}
                            )
                            SELECT *
                            FROM all_data
                            """.rstrip(),
                    aws_conn_id="aws_conn",
                    s3_bucket=s3_data_bucket,
                    s3_key=f"""{s3_data_prefix}/{schema_name}/{table_name}/{{{{ task_instance.xcom_pull(key="{schema_name}.{table_name}.filename") }}}}""",
                    batch_min_rows=batch_min_rows,
                    method=method,
                    columns_to_hash=columns_to_hash,
                    # pd_csv_kwargs={'compression': 'gzip'}
                    # pd_csv_kwargs={'na_rep': 'NULL', 'compression': 'gzip', 'chunksize': 10000}
                )
            else:
                full_extract_from_pg_load_to_s3_task = PostgresToS3Operator(
                    task_id=f"extract_from_pg_load_to_s3",
                    postgres_conn_id=source_conn_id,
                    query=f"""SELECT * 
                             FROM ({sql}) q""",
                    aws_conn_id="aws_conn",
                    s3_bucket=s3_data_bucket,
                    s3_key=f"""{s3_data_prefix}/{schema_name}/{table_name}/{{{{ task_instance.xcom_pull(key="{schema_name}.{table_name}.filename") }}}}""",
                    batch_min_rows=batch_min_rows,
                    method=method,
                    columns_to_hash=columns_to_hash,
                    # pd_csv_kwargs={'compression': 'gzip'}
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
                columns_array=f"""{{{{ task_instance.xcom_pull(key="{schema_name}.{table_name}.columns_list") }}}}""".split(
                    ","),
                file_format=f"(format_name={file_format})"
            )

            log_started = PostgresOperator(
                task_id=f'log_started',
                postgres_conn_id="data-utils-db",
                sql=f"""INSERT INTO etl_logs(connection_id,schema_name,table_name,dag_name,event_ts,status)
                         VALUES (
                                '{source_conn_id}',
                                '{schema_name}',
                                '{table_name}',
                                '{dag_name}',
                                CURRENT_TIMESTAMP,
                                'STARTED'
                                )"""
            )

            log_completed = PostgresOperator(
                task_id=f'log_completed',
                postgres_conn_id="data-utils-db",
                sql=f"""INSERT INTO etl_logs(connection_id,schema_name,table_name,dag_name,filename_key,event_ts,status)
                                    VALUES ( '{source_conn_id}',
                                            '{schema_name}',
                                            '{table_name}',
                                            '{dag_name}',
                                            '{s3_data_prefix}/{schema_name}/{table_name}/{{{{ task_instance.xcom_pull(key="{schema_name}.{table_name}.filename") }}}}',
                                            CURRENT_TIMESTAMP,
                                            'COMPLETED')"""
            )

            if load_method in ["merge", "insert-only"]:
                # merge or insert-only
                if order_field is not None:
                    # extract incremental
                    log_started >> get_target_table_info_task >> inc_extract_from_pg_load_to_s3_task >> create_stage_table >> copy_into_table_task >> merge_task >> log_completed
                else:
                    # extract full
                    log_started >> get_target_table_info_task >> full_extract_from_pg_load_to_s3_task >> create_stage_table >> copy_into_table_task >> merge_task >> log_completed

            elif load_method == "truncate-insert":
                # truncate-insert
                log_started >> get_target_table_info_task >> full_extract_from_pg_load_to_s3_task >> truncate_table >> copy_into_table_task >> log_completed

    return dag


env_config = Variable.get("env_config", deserialize_json=True)
config = get_config(env_config, "config")
dag = build_dag_from_config(config)
with dag:
    print(dag.dag_id)



