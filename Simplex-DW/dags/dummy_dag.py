from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define the DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    dag_id="mock_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

# Define the task
end_task = DummyOperator(
    task_id="end_task",
    dag=dag,
)
