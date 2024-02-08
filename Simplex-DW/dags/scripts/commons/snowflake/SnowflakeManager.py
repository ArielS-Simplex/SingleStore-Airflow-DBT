import logging as log
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import scripts.commons.snowflake.queries as snflk_queries
import snowflake.connector.pandas_tools as pt
import pandas as pd

dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")


class SnowflakeManager:
    """ DataLake Snowflake Manager """

    @staticmethod
    def execute_snowflake_query(target, stage, pattern, file_format='PARQUET', s3_path=None, **kwargs):
        sql_statements = SnowflakeManager.get_copy_sql_statement(target, stage, pattern, file_format, s3_path).split(
            ";")
        with SnowflakeManager.get_snowflake_conn().cursor() as cursor:
            for sql in sql_statements:
                if sql.strip():  # avoid executing empty statements
                    cursor.execute(sql)

    @staticmethod
    def get_snowflake_conn():
        """ Get snowflake connection object """
        return dwh_hook.get_conn()

    @staticmethod
    def get_copy_sql_statement(target: str, stage: str, pattern: str, file_format: str = 'PARQUET',
                               s3_path: str = None) -> str:
        """ Get COPY SQL statement for data ingestion to Snowflake DWH """

        return f'''
            alter stage {stage} set url = '{s3_path}' ;
                
            COPY INTO {target} 
            FROM (
               select
                METADATA$FILENAME::string ,
                $1::variant,
                CURRENT_TIMESTAMP
                from @{stage}) 
                pattern='{pattern}' 
                FILE_FORMAT = ( TYPE = {file_format} ); 
'''

    @staticmethod
    def get_max_order_field(schema_name, table_name, order_field='', backward_update_period=None):
        """ get_max_order_field from target table """

        snowflake_query = f"SELECT CURRENT_TIMESTAMP,COUNT(*) FROM {schema_name}.{table_name}"

        if order_field is not None:
            snowflake_query = f"SELECT COALESCE(MAX({order_field}),'0'),COUNT(*) FROM {schema_name}.{table_name}"

            if backward_update_period is not None and order_field.lower() in ['created_at', 'inserted_at', 'updated_at',
                                                                              'valid_until']:
                snowflake_query = f"""
                                   SELECT MAX({order_field}) - interval '{backward_update_period}' AS max_date 
                                          ,COUNT(*) table_count
                                    FROM {schema_name}.{table_name} ;"""
                if 'days' in backward_update_period:
                    snowflake_query = f"""
                       select 
                           case when current_timestamp - interval '{backward_update_period}' > max_date then 
                               max_date 
                           else 
                               current_timestamp - interval '{backward_update_period}' 
                           end max_order_id,
                           table_count
                       from (
                           SELECT COALESCE(MAX({order_field}),'0') max_date ,COUNT(*) table_count
                           FROM {schema_name}.{table_name}
                       );"""

        result = dwh_hook.get_first(snowflake_query)
        max_order_id = str(result[0])
        table_count = str(result[1])

        return max_order_id, table_count

    @staticmethod
    def columns_list_query(schema_name, table_name) -> str:
        """ bring  list of columns from table """
        columns_list_query = f"""select listagg(column_name , ', ') within group (ORDER BY ordinal_position)
                        from information_schema.columns
                        where lower(table_name)='{table_name}' 
                              and lower(table_schema)='{schema_name}'
                              and column_default is null"""
        result = dwh_hook.get_first(columns_list_query)
        return str(result[0])

    @staticmethod
    def merge_query(load_method, schema_name, table_name, key_field) -> str:
        """  create merge_query for sync_pg """
        if load_method in ["merge", "insert-only"]:
            key_field = key_field.lower()
            if load_method == "merge":
                merge_query = f"""
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
                merge_query = f"""
                                        select  'merge into ' || table_name || ' AS target using stage.' || table_schema || '_' || table_name || ' AS source on target.{key_field} = source.{key_field}\r\n' ||
                                                'when not matched then insert ({key_field},' || listagg(column_name, ', ') within group (ORDER BY ordinal_position) || ') values (source.{key_field},' || listagg('source.' || column_name, ', ') within group (ORDER BY ordinal_position) || ')'
                                        from information_schema.columns
                                        where lower(table_name)='{table_name}' and lower(table_schema)='{schema_name}' 
                                            and lower(column_name) not in ('{key_field}')
                                            and column_default is null
                                        group by table_name,table_schema
                                        """
            result = dwh_hook.get_first(merge_query)
            merge_command = str(result[0])
        else:
            merge_command = ''

        return merge_command

    @staticmethod
    def upsert_chargebacks_to_snowflake(data: pd.DataFrame) -> None:
        # log.info('Writing chargebacks rows to Snowflake table')
        try:
            # log.info('Getting connection to Snowflake')
            snowflk_conn = SnowflakeManager.get_snowflake_conn()
            # log.info('Connection to Snowflake acquired')
            cursor = snowflk_conn.cursor()
            # log.info('Creating and switching to salesforce schema')
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS SALESFORCE')
            cursor.execute(f'USE SCHEMA SALESFORCE')
            # log.info('Creating CHARGEBACKS_LOG table if not exists')
            cursor.execute(snflk_queries.CHARGEBACKS_LOG_CREATE)
            # log.info('Creating CHARGEBACKS_LOG_TEMP table')
            cursor.execute(snflk_queries.CHARGEBACKS_LOG_TEMP_CREATE)
            # log.info('Writing data to CHARGEBACKS_LOG_TEMP table')
            pt.write_pandas(conn=snowflk_conn, df=data, table_name='CHARGEBACKS_LOG_TEMP', schema='SALESFORCE',
                            quote_identifiers=False)
            # log.info('Finished writing data to CHARGEBACKS_LOG_TEMP table')
            # log.info('Merge data from CHARGEBACKS_LOG_TEMP to CHARGEBACKS_LOG table')
            cursor.execute(snflk_queries.CHARGEBACKS_LOG_MERGE)
            # log.info('Finished merging to CHARGEBACKS_LOG_TEMP table')

        except Exception as e:
            raise Exception(f"Could not write chargebacks data to Snowflake. Reason: {e}")
        finally:
            snowflk_conn.close()

    @staticmethod
    def get_latest_chargeback_update_time() -> str:
        try:
            conn = SnowflakeManager.get_snowflake_conn()
            cursor = conn.cursor()
            table_exists = cursor.execute(snflk_queries.CHECK_IF_CHARGEBACKS_LOG_EXISTS).fetchone()[0]
            if table_exists:
                return cursor.execute(snflk_queries.CHARGEBACKS_LOG_LAST_MODIFIED).fetchone()[0]
            else:
                # if table is empty load all data
                return '951058224'
        except Exception as e:
            raise Exception(f"Could not fetch latest update timestamp from chargebacks. Reason: {e}")
        finally:
            conn.close()

    @staticmethod
    def validation_columns_query(config, notifier) -> str:
        """   """
        table_name = config['table_name']
        columns = config['columns']
        db_and_schema_source = config['db_and_schema_source']
        db_and_schema_target = config['db_and_schema_target']
        where_query = config['where_query']

        query = f""" select {columns}
                     FROM {db_and_schema_source}.{table_name}
                     WHERE {where_query}
                     EXCEPT
                     SELECT {columns}
                     FROM {db_and_schema_target}.{table_name}
                     WHERE {where_query}
                                """
        result = dwh_hook.get_pandas_df(sql=query)
        log.info(query)
        delta = len(result)

        if delta > 0:
            log.info(result)
            payload = f'value difference at {db_and_schema_target}.{table_name}_' + str(delta)
            notifier.send_general_notification(message_text=payload)

    @staticmethod
    def validation_row_count(config, notifier) -> str:
        """   """
        db_and_schema_source = config['db_and_schema_source']
        source_table = config['source_table']
        where_source = config.get('where_source', '')
        where_target = config.get('where_target', '')

        query = f""" SELECT sum(count_rows)
                     FROM (SELECT count(1) AS count_rows
                           FROM {db_and_schema_source}.{source_table}
                                {where_source}
                           UNION
                           SELECT -1 * count(1) AS count_rows
                           FROM {config['db_and_schema_target']}.{config['target_table']}
                                {where_target} ) as a
                                """
        result = dwh_hook.get_first(sql=query)
        # log.info(query)
        if result[0] != 0:
            log.info('row difference_____' + str(result[0]))
            payload = f'row difference_' + str(result[0]) + f' at {db_and_schema_source}.{source_table}'
            notifier.send_general_notification(message_text=payload)

    @staticmethod
    def validation(config, notifier) -> str:
        SnowflakeManager.validation_columns_query(config, notifier)
        SnowflakeManager.validation_row_count(config, notifier)

    @staticmethod
    def de_duplication(target_schema, target_table) -> str:
        """ duplicate id's from source tables """
        de_duplication_query = f"""
                DELETE FROM {target_schema}.{target_table} tab1
                    USING (
                        SELECT ROW_NUMBER() OVER (PARTITION BY DATA:id ORDER BY DW_CREATED_AT DESC) row_n, *
                        FROM {target_schema}.{target_table}
                        WHERE DW_CREATED_AT >= (CURRENT_TIMESTAMP - INTERVAL '1 DAY')
                        ) tab2
                WHERE row_n > 1
                AND tab1.DATA:id = tab2.DATA:id
                AND tab1.DW_CREATED_AT = tab2.DW_CREATED_AT;
            """
        result = dwh_hook.get_first(de_duplication_query)
        return str(result[0])
