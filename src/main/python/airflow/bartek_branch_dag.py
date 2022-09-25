import random

from airflow import DAG
import pendulum
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


def _my_python_branch():
    return random.choice(['branch1', 'branch2'])


with DAG(dag_id='bartek_branch_dag',
         schedule_interval="_SCHEDULE_INTERVAL_",
         start_date=pendulum.parse('_START_DATE_', tz='UTC'),

         # optional below
         default_args={},
         description='Bartek branch dag description',
         tags=["bartek"],
         ) as dag:
    t = BranchPythonOperator(task_id='branching', python_callable=_my_python_branch)
    t1 = BashOperator(task_id='branch1', bash_command='echo "Branch1"')
    t2 = BashOperator(task_id='branch2', bash_command='echo "Branch2"')
    join = DummyOperator(task_id='join', trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)

    t >> [t1, t2] >> join

# echo "Current local date time: $(date '+%Y-%m-%dT%H:%M:%S%z')" && \
# echo "Current UTC   date time: $(date -u '+%Y-%m-%dT%H:%M:%S%z')" && \
# cat bartek_branch_dag.py | \
# sed "s/_SCHEDULE_INTERVAL_/$(date -u -v +3M '+%M %H %d') * */" | \
# sed "s/_START_DATE_/$(date -u -v -1m -v +3M +%Y-%m-%dT%H:%M:00)/"