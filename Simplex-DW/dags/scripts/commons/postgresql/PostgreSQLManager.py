import psycopg2.extras as extras
import scripts.commons.postgresql.queries.salesforce_queries as sq
from scripts.commons.consts import Consts as cnst
from airflow.providers.postgres.hooks.postgres import PostgresHook
from logging import info

class PostgreSQLManager:
    """ PostgreSQL Manager """
    @staticmethod
    def colnames(conn, table):
        """ Get column names for table"""
        cursor = conn.cursor()
        cursor.execute(f"Select * FROM {table} LIMIT 0")
        colnames = [desc[0] for desc in cursor.description]
        mystring = ''
        for x in colnames:
            mystring += ' ,'+x

        return mystring[2:]


    @staticmethod
    def insert_df_into_pg(conn, dataframe, table):
        """ transfer dataframe into pg database"""
        # Creating a list of tuples from the dataframe values
        tuples = [tuple(x) for x in dataframe.to_numpy()]

        # dataframe columns with Comma-separated
        cols = ','.join(list(dataframe.columns))

        # SQL query to execute
        sql = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = conn.cursor()
        extras.execute_values(cursor, sql, tuples)
        # info("Data inserted using execute_values() successfully..")

    @staticmethod
    def insert_2_etl_logs(status=None, config_data=None, s3_keys=None, **context):
        data_utils_db_hook = PostgresHook(postgres_conn_id="data-utils-db")
        target_table = cnst.LOG_TABLE
        dag_name = context["dag"].dag_id
        table_name = config_data['table_name']
        schema_name = config_data['schema_name']
        schema_and_table = schema_name+'_'+table_name
        source_conn_id = config_data.get('source_conn_id', '')
        # info(config_data)

        if status == 'log_started':
            sql_query = f"""
                         INSERT INTO public.{target_table}(connection_id,schema_name,table_name,dag_name,event_ts,status)
                         VALUES (
                                '{source_conn_id}',
                                '{schema_name}',
                                '{table_name}',
                                '{dag_name}',
                                CURRENT_TIMESTAMP,
                                'STARTED'
                                )"""

        if status == 'log_exception':
            log_url = context.get('task_instance').log_url
            exception = str(context['exception']).replace("'", "")
            sql_query = f"""
                                 INSERT INTO public.{target_table}(connection_id,schema_name,table_name,dag_name
                                                                  ,filename_key,event_ts,status,error_message)
                                      select connection_id,schema_name,table_name,dag_name
                                      ,filename_key ,CURRENT_TIMESTAMP, 'FAILED' ,'{exception}\n{log_url}'
                                      from (
                                            select row_number() over(partition by schema_name,
                                            table_name order by event_ts desc) row_num,*
                                            from public.{target_table}
                                            where schema_name || '_' || table_name ='{schema_and_table}'
                                                  and status='STARTED'
                                            ) {target_table}
                                      where row_num = 1;    """
        if status == 'log_completed':
            if s3_keys is None:
                s3_keys = 'no need to save file'

            sql_query = f"""INSERT INTO {target_table}(connection_id,schema_name,table_name,dag_name,filename_key,
                                                       event_ts, status)
                                    VALUES ('{source_conn_id}',
                                            '{schema_name}',
                                            '{table_name}',
                                            '{dag_name}',
                                            '{s3_keys}',
                                            CURRENT_TIMESTAMP,
                                            'COMPLETED')"""
        # info(sql_query)
        data_utils_db_conn = data_utils_db_hook.get_conn()
        cursor = data_utils_db_conn.cursor()
        cursor.execute(sql_query)
        data_utils_db_conn.commit()
        cursor.close

        return

    @staticmethod
    def __pg_query_high_volume(schema_name, table_name, sql_condition, batch_max_rows, max_order_id,order_field=None
                               , volume="", extra_where_high_volume="") -> str:
        """  create query for high_volume tables  """
        if volume.lower() == "high":
            if order_field.lower() in ['created_at', 'inserted_at', 'updated_at', 'valid_until', 'lastsrequest_requesttime']:
                extra_where_high_volume = f'''
                                          WHERE  {order_field} >{max_order_id} {sql_condition}
                                            AND {order_field} < date_trunc('minute',(select max({order_field}) AS max_date from(
                                                                                                            select {order_field}
                                                                                                            FROM {schema_name}.{table_name} 
                                                                                                            WHERE {order_field} >{max_order_id}
                                                                                                            order by {order_field} asc
                                                                                                            limit {batch_max_rows}) as m) - interval '1 minutes')
                                                                                                            '''
            else:
                extra_where_high_volume = f"""WHERE {order_field} <= (SELECT max({order_field}) FROM {schema_name}.{table_name} """
        return extra_where_high_volume

    @staticmethod
    def pg_extract_query(schema_name, table_name, sql, sql_condition, batch_max_rows, max_order_id, order_field=None
                         , volume="") -> str:
        """ create query to extract data from pg"""
        if order_field is not None:
            # data can be extract incrementally only if order_field is available
            extra_where_high_volume = PostgreSQLManager.__pg_query_high_volume(schema_name, table_name, sql_condition,
                                                                               batch_max_rows, max_order_id, order_field,
                                                                               volume)

            query = f"""
                    WITH all_data AS
                    (
                        SELECT *
                        FROM ({sql} {extra_where_high_volume}) q
                        WHERE {order_field} > {max_order_id}
                        ORDER BY {order_field} ASC
                        LIMIT {batch_max_rows}
                    )
                    SELECT *
                    FROM all_data
                    """.rstrip()
        else:
            query = f"""SELECT *  FROM ({sql}) q""".rstrip()

        return query


    @staticmethod
    def get_salesforce_query(query_name: str) -> str:
        """ Get chargebacks data from Postgres"""
        return getattr(sq, query_name)
