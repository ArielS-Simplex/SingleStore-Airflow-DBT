from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager as pg
from airflow.models import Variable
import pandas as pd
import logging


class Snowflake2PostgresManager:
    """ snowflake_postgres Manager """

    @staticmethod
    def snowflake_to_postgres(pg_conn, pg_hook, snow_conn, config):
        logging.info(config)
        source_table = config['source_table']
        staging_table = config['staging_table']
        target_table = config['target_table']
        date_field = config['date_field']

        env_config = Variable.get("env_config", deserialize_json=True)
        var_sf_to_pg_chunksize = env_config['var_sf_to_pg_chunksize']
        var_de_database = env_config['var_de_database']

        col_names = pg.colnames(conn=pg_conn, table=staging_table)

        sql_max_query = f''' select date_trunc('hour', max({date_field}) - interval '12 hour')::text
                             from {target_table}  '''
        max_date = (pg_hook.get_first(sql=sql_max_query)[0])
        if max_date is None:
            max_date = '1970-01-01'

        sql_query = f''' select {col_names}
                         from {var_de_database}.{source_table}
                         where time>'{max_date}'
                         order by time
                         '''

        for df in pd.read_sql(sql=sql_query, con=snow_conn, chunksize=var_sf_to_pg_chunksize):
            pg.insert_df_into_pg(pg_conn, df, staging_table)
            pg_conn.commit()
