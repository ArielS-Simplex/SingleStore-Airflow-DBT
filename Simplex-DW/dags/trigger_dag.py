from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
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
    dag_id="trigger_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

# Define the ExternalTaskSensor
wait_for_mock_dag = ExternalTaskSensor(
    task_id="wait_for_end_task_in_mock_dag",
    external_dag_id="mock_dag",
    external_task_id="end_task",
    execution_delta=timedelta(days=1),  # Adjust as needed
    check_existence=True,
    allowed_states=['failed'],  # You can customize this list if needed
    poke_interval=10,
    dag=dag,
)
