from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from logging import info
from tabulate import tabulate
import pandas as pd


def snowflake_to_s3(query, s3_bucket, s3_key, aws_conn_id='aws_conn_id'):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    snow_conn = dwh_hook.get_conn()

    info(query)
    # df = dwh_hook.get_pandas_df(sql=query)
    df = pd.read_sql(sql=query, con=snow_conn)

    info(tabulate(df, headers='keys', tablefmt='psql'))

    info("Data from snowflake obtained")
    csv_data = df.to_csv(index=False)

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_string(
        string_data=csv_data,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True)
    info("Data was upload to s3")

    return
