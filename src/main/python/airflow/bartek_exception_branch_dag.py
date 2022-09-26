import random
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def _my_python_branch():
    return random.choice(['airflow_fail_exception_branch', 'exception_branch'])


def _python_airflow_fail_exception_callable():
    raise AirflowFailException('AirflowFailException from airflow_fail_exception_branch')


def _python_exception_callable():
    raise Exception('Exception from exception_branch')


with DAG(dag_id='bartek_exception_branch_dag',
         schedule_interval="_SCHEDULE_INTERVAL_",
         start_date=pendulum.parse('_START_DATE_', tz='UTC'),
         default_args={
             'retries': 1,
             'retry_delay': timedelta(minutes=1)
         },
         description='Bartek exception branch dag description',
         tags=["bartek"],

         ) as dag:
    t = BranchPythonOperator(task_id='branching', python_callable=_my_python_branch)
    t1 = PythonOperator(task_id='airflow_fail_exception_branch', python_callable=_python_airflow_fail_exception_callable)
    t2 = PythonOperator(task_id='exception_branch', python_callable=_python_exception_callable)
    join = DummyOperator(task_id='join', trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)

    t >> [t1, t2] >> join

# echo "Current local date time: $(date '+%Y-%m-%dT%H:%M:%S%z')" && \
# echo "Current UTC   date time: $(date -u '+%Y-%m-%dT%H:%M:%S%z')" && \
# cat bartek_exception_branch_dag.py | \
# sed "s/_SCHEDULE_INTERVAL_/$(date -u -v +3M '+%M %H %d') * */" | \
# sed "s/_START_DATE_/$(date -u -v -1m -v +3M +%Y-%m-%dT%H:%M:00)/"