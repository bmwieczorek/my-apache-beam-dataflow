import logging
import pendulum
from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def _setup(**kwargs):
    task_instance: TaskInstance = kwargs['task_instance']
    logging.info(f"task_instance={task_instance}")
    dag_run: DagRun = kwargs["dag_run"]
    logging.info(f"dag_run={dag_run}")
    dag_run_conf = dag_run.conf
    logging.info(f"dag_run_conf={dag_run_conf}")
    if "my_param" in dag_run_conf:
        my_param = dag_run_conf["my_param"]
        logging.info(f"my_param={my_param}")
        task_instance.xcom_push(key="my_param", value=my_param)
    else:
        task_instance.xcom_push(key="my_param", value=None)


def _has_param_branch(**context):
    logging.info(f"context={context}")
    task_instance: TaskInstance = context['task_instance']
    logging.info(f"task_instance={task_instance}")
    my_param = task_instance.xcom_pull(task_ids="setup", key="my_param")
    logging.info(f"my_param={my_param}")
    task_instance.xcom_push(key="my_param", value=my_param)
    return "skip_prepare_temp" if my_param is None else "has_data_branch"


def _has_data_branch(**context):
    logging.info(f"context={context}")
    task_instance: TaskInstance = context['task_instance']
    my_param = task_instance.xcom_pull(task_ids="has_param_branch", key="my_param")
    logging.info(f"my_param={my_param}")
    return "prepare_temp" if my_param == 'true' else "no_data_end"


with DAG(dag_id='bartek_xcom_branch_dag',
         schedule_interval="_SCHEDULE_INTERVAL_",
         start_date=pendulum.parse('_START_DATE_', tz='UTC'),
         # optional below
         default_args={}, description='Bartek xcom branch dag description', tags=["bartek"],
         ) as dag:
    setup = PythonOperator(task_id="setup", provide_context=True, python_callable=_setup)
    has_param_branch = BranchPythonOperator(task_id='has_param_branch', provide_context=True, python_callable=_has_param_branch)
    has_data_branch = BranchPythonOperator(task_id='has_data_branch', provide_context=True, python_callable=_has_data_branch)
    prepare_temp = BashOperator(task_id='prepare_temp', bash_command='echo "prepare_temp"')
    skip_prepare_temp = DummyOperator(task_id='skip_prepare_temp')
    process = BashOperator(task_id='process', bash_command='echo "process"', trigger_rule=TriggerRule.ONE_SUCCESS)
    post_process = BashOperator(task_id='post_process', bash_command='echo "post_process"')
    no_data_end = DummyOperator(task_id='no_data_end')

    setup >> has_param_branch >> [has_data_branch, skip_prepare_temp]
    has_data_branch >> [prepare_temp, no_data_end]
    skip_prepare_temp >> process
    prepare_temp >> process
    process >> post_process
