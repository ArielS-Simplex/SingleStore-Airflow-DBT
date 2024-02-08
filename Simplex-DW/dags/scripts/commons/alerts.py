from slack import WebClient
from airflow.models import Variable

slack_token = Variable.get('AIRFLOW_VAR_SLACK_TOKEN')
channel_id = Variable.get('AIRFLOW_VAR_SLACK_CHANNEL_ID')
slack_client = WebClient(token=slack_token)


def task_fail_slack_alert(context):
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    failed_alert = slack_client.chat_postMessage(
        channel=channel_id,
        text=slack_msg)
    return failed_alert.execute(context=context)