from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'poc_upload_to_singlestore',
    default_args=default_args,
    description='Upload local files to SingleStore',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
)


def upload_to_singlestore(df, table_name, connection_string):
    """
    Uploads a DataFrame to a SingleStore table.

    :param df: Pandas DataFrame to upload
    :param table_name: Name of the table in SingleStore
    :param connection_string: SQLAlchemy connection string for SingleStore
    """
    engine = sqlalchemy.create_engine(connection_string)
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    engine.dispose()


def upload_files_to_singlestore():
    directory_path = "/ariel_table_test"  # Updated to a directory path
    table_name = "ariel_table_upload"
    connection_string = "mysql+pymysql://dev_user:Qwe123456@dev-tlv-memagg01.gw-4u.com:3306/ods"

    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path)
            upload_to_singlestore(df, table_name, connection_string)


upload_task = PythonOperator(
    task_id='upload_files_to_singlestore',
    python_callable=upload_files_to_singlestore,
    dag=dag,
)

upload_task
