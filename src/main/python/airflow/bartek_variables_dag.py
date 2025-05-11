from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.models import DagRun, TaskInstance, Variable
from airflow.operators.python import PythonOperator


# https://bmwieczorek.wordpress.com/2023/04/05/airflow-overwrite-dag-parameters-arguments-variables/

def my_python_operator(*args, **kwargs):
    print(f"hello {args} from {kwargs} from python")
    if 'dag' in kwargs:
        d: DAG = kwargs['dag']
        print(f"d.dag_id={d.dag_id}")
    if 'dag_run' in kwargs:
        dr: DagRun = kwargs['dag_run']
        # Trigger dag configuration JSON (Optional)
        print(f"dr.conf={dr.conf}")  # {"my_param": 1} when manually triggered dag with {'my_param': 1}
    if 'task_instance' in kwargs:
        ti: TaskInstance = kwargs['task_instance']
    # admin variables
    my_airflow_admin_overwrite_variable = Variable.get('my_airflow_admin_overwrite_variable', None)
    print(f"my_airflow_admin_overwrite_variable={my_airflow_admin_overwrite_variable}")
    return kwargs['city']


with DAG(dag_id='bartek_variables_dag',
         schedule_interval=timedelta(minutes=3),
         start_date=pendulum.utcnow().subtract(minutes=3),  # optionally specify end date
         end_date=pendulum.utcnow(),

         # optional below
         description='Bartek variables dag description',
         tags=["bartek"],
         ) as dag:
    t = PythonOperator(task_id='my_python_with_context', python_callable=my_python_operator, op_args=["Bartek"],
                       op_kwargs={'city': 'NY'}, provide_context=True)
