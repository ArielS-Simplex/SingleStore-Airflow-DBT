"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import datetime

from airflow import DAG, settings

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.jobs.base_job import BaseJob
from airflow.models import DAG, DagModel, DagTag, DagRun, ImportError, Log, SlaMiss, RenderedTaskInstanceFields, TaskFail, \
    TaskInstance, TaskReschedule, Variable, XCom

from datetime import timedelta, timezone
import os
import logging

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'retries': 0
}

DEFAULT_MAX_AGE_IN_DAYS = 31
DEFAULT_CLEANUP_MODE = 'delete'

def check_cleanup_mode():
    cleanup_mode = Variable.get("mwaa_db_cleanup_mode", default_var=DEFAULT_CLEANUP_MODE)

    if cleanup_mode == DEFAULT_CLEANUP_MODE:
        objects_to_clean = [[BaseJob, BaseJob.latest_heartbeat],
                            #DagTag is missing foreign key in Airflow (will be fixed in future versions), hence the next line.
                            #We need to delete the tags, so we filter on future date
                            [DagTag, datetime.datetime(1970, 1, 1, tzinfo=timezone.utc)],
                            [DagModel, DagModel.last_parsed_time],
                            [DagRun, DagRun.execution_date],
                            [ImportError, ImportError.timestamp],
                            [Log, Log.dttm],
                            [SlaMiss, SlaMiss.execution_date],
                            [RenderedTaskInstanceFields, RenderedTaskInstanceFields.execution_date],
                            [TaskFail, TaskFail.start_date],
                            [TaskInstance, TaskInstance.execution_date],
                            [TaskReschedule, TaskReschedule.execution_date],
                            [XCom, XCom.execution_date],
                            ]
    elif cleanup_mode == 'truncate':
        objects_to_clean = [[BaseJob],
                            [DagTag],
                            [DagModel],
                            [DagRun],
                            [ImportError],
                            [Log],
                            [SlaMiss],
                            [RenderedTaskInstanceFields],
                            [TaskFail],
                            [TaskInstance],
                            [TaskReschedule],
                            [XCom],
                            ]
    else:
        raise Exception(f'Cleanup mode not recognized! Expecting delete or truncate, got {cleanup_mode}')
    return objects_to_clean, cleanup_mode

def cleanup_db_fn(**kwargs):
    session = settings.Session()
    print("session: ", str(session))

    oldest_date = days_ago(int(Variable.get("max_metadb_storage_days", default_var=DEFAULT_MAX_AGE_IN_DAYS)))
    print("oldest_date: ", oldest_date)

    objects_to_clean, cleanup_mode = check_cleanup_mode()

    logging.info(f"Starting cleanup. Running in mode - {cleanup_mode}. Cleaning objects - {objects_to_clean}")
    for x in objects_to_clean:
        if cleanup_mode == DEFAULT_CLEANUP_MODE:
            query = session.query(x[0]).filter(x[1] <= oldest_date)
        else:
            query = session.query(x[0])
        print(str(x[0]))
        query.delete(synchronize_session=False)

    session.commit()

    return "OK"


with DAG(
        dag_id=os.path.basename(__file__).replace(".py", ""),
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval='@daily',
        tags=['db']
) as dag:
    cleanup_db = PythonOperator(
        task_id="cleanup_db",
        python_callable=cleanup_db_fn,
        provide_context=True
    )