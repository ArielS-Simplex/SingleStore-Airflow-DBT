from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests

# DAG Arguments
ARGS = {
    "owner": "Airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    dag_id="dbt_rpc_singlestore_run",
    default_args=ARGS,
    description="Run DBT Model via DBT RPC",
    schedule_interval=None,  # Set to None for manual trigger
)

def test_dbt_connection():
    dbt_url = 'http://172.23.0.2:8580/jsonrpc'  # Replace 'dbt' with your DBT service name in docker-compose
    try:
        response = requests.get(dbt_url)
        response.raise_for_status()
        print("DBT Docker connection successful")
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to DBT Docker: {e}")

def trigger_dbt_run():
    dbt_url = 'http://172.23.0.2:8580/jsonrpc'  # Adjust if needed
    headers = {'Content-Type': 'application/json'}
    data = {
        "jsonrpc": "2.0",
        "method": "run",  # Or other DBT command
        "id": 1,
        "params": {
            "models": "POC_DimClients.sql"
        }
    }
    try:
        response = requests.post(dbt_url, json=data, headers=headers)
        response.raise_for_status()
        print("DBT run triggered successfully")
        print(f"DBT Run Response: {response.json()}")
    except requests.exceptions.RequestException as e:
        print(f"Error triggering DBT run: {e}")



# Define your tasks
test_connection_task = PythonOperator(
    task_id='test_dbt_connection',
    python_callable=test_dbt_connection,
    dag=dag
)

trigger_dbt_task = PythonOperator(
    task_id='trigger_dbt_run',
    python_callable=trigger_dbt_run,
    dag=dag
)

# Task Order
test_connection_task >> trigger_dbt_task
