from datetime import timedelta
import time
import pendulum
from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

# pip install apache-airflow==1.10.15

# pip install apache-airflow-backport-providers-google==2021.3.3
# includes:
# google-auth==1.35.0
# google-cloud-secret-manager==1.0.2
# google-cloud-bigquery==2.34.4


BUCKET = ""
SERVICE_ACCOUNT = ""
BIGQUERY_PROJECT_ID = ""
BIGQUERY_TABLE = ""
SECRET_MANAGER_PROJECT_ID = ""
SECRET_NAME = ""
DATAFLOW_PROJECT_ID = ""
DATAFLOW_SUBNETWORK = ""
DATAFLOW_TEMP_LOCATION = ""
DATAFLOW_INPUT = ""
DATAFLOW_OUTPUT = ""
DATAFLOW_TEMPLATE = ""
EMAIL = ""


def my_python_operator(*args, **kwargs):
    print(f"hello {args} from {kwargs} from python")
    if 'dag' in kwargs:
        d: DAG = kwargs['dag']
        print(f"d.params={d.params}")
        print(f"d.dag_id={d.dag_id}")
        print(f"d.catchup={d.catchup}")
        print(f"d.max_active_runs={d.max_active_runs}")
        print(f"d.tags={d.tags}")
        print(f"d.start_date={d.start_date}")
        print(f"d.schedule_interval={d.schedule_interval}")
        # print(f"d.={d.}")
    if 'dag_run' in kwargs:
        dr: DagRun = kwargs['dag_run']
        print(f"dr.dag_id={dr.dag_id}")
        print(f"dr.start_date={dr.start_date}")
        print(f"dr.conf={dr.conf}")  # Bigf={'my_param': 1} when manually tiggered dag with {'my_param': 1}
        print(f"dr.end_date={dr.end_date}")
        print(f"dr.id={dr.id}")
        print(f"dr.execution_date={dr.execution_date}")
        print(f"dr.dag={dr.dag}")
        print(f"dr.external_trigger={dr.external_trigger}")
        # print(f"dr.={dr.}")
    if 'task_instance' in kwargs:
        ti: TaskInstance = kwargs['task_instance']
        print(f"ti.dag_id={ti.dag_id}")
        print(f"ti.start_date={ti.start_date}")
        print(f"ti.execution_date={ti.execution_date}")
        print(f"ti.end_date={ti.end_date}")
        print(f"ti.duration={ti.duration}")
        print(f"ti.operator={ti.operator}")
        print(f"ti.task_id={ti.task_id}")
        print(f"ti.state={ti.state}")
        # print(f"ti.={ti.}")
        # ti.xcom_push(key="", value="")
        # value = ti.xcom_pull(task_ids="", key="")
    return kwargs['city']


def my_python_gcs_list_operator():
    operator = GCSListObjectsOperator(task_id='my_python_gcs_list_operator', bucket=BUCKET, prefix='', delimiter=".*")
    object_names = operator.execute(None)
    for object_name in object_names:
        print(f"object_name={object_name}")


def my_python_gcs_list_bash_operator_gsutil():
    time_millis = round(time.time() * 1000)
    impersonation_opts = f"-i {SERVICE_ACCOUNT}"
    gsutil_state_opts = f"-o 'GSUtil:state_dir=/tmp/bartek_dag_gsutil_state_{time_millis}'"
    command = f"gsutil {gsutil_state_opts} {impersonation_opts} ls {BUCKET}"
    operator = BashOperator(task_id='my_python_gcs_list_bash_operator_gsutil', bash_command=command)
    operator.execute(dict())


def my_python_bq_select_count_client():
    from google.cloud import bigquery
    query = f"SELECT * FROM `{BIGQUERY_PROJECT_ID}` LIMIT 10"

    # client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    # query_job = client.query(query=query, location='US')
    # client = bigquery.Client()
    # query_job = client.query(query=query, location='US', project=BIGQUERY_PROJECT_ID)

    impersonated_credentials = _get_impersonated_credentials(
        ['https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/bigquery'])

    # noinspection PyTypeChecker
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID, credentials=impersonated_credentials)
    query_job = client.query(query=query, location='US')

    results = query_job.result()
    print(f"Got {results.total_rows}")


def _get_impersonated_credentials(target_scopes):
    from google.auth import _default, impersonated_credentials
    source_credentials, project_id = _default.default(scopes=target_scopes)
    target_credentials = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=SERVICE_ACCOUNT,
        target_scopes=target_scopes,
        lifetime=600)
    return target_credentials


def my_python_secret_manager_access_client():
    from google.cloud import secretmanager
    impersonated_credentials = _get_impersonated_credentials(['https://www.googleapis.com/auth/cloud-platform'])
    client = secretmanager.SecretManagerServiceClient(credentials=impersonated_credentials)
    secret_path = client.secret_version_path(SECRET_MANAGER_PROJECT_ID, SECRET_NAME, 'latest')
    response = client.access_secret_version(name=secret_path)
    payload = response.payload.data.decode("UTF-8")
    print("Plaintext: {}".format(payload))


def my_python_dataflow_templated_operator():
    from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
    user_labels = {
        "owner": "bartek"
    }
    job_env = {
        "additionalUserLabels": user_labels,
        "serviceAccountEmail": SERVICE_ACCOUNT
    }
    operator = DataflowTemplatedJobStartOperator(
        task_id="my",
        template=DATAFLOW_TEMPLATE,
        project_id=DATAFLOW_PROJECT_ID,
        job_name="bartek-job-from-template-airflow",
        environment=job_env,
        location="us-central1",
        parameters={
            "input": DATAFLOW_INPUT,
            "output": DATAFLOW_OUTPUT,
            "patterns": 'TIM="1649140017533";END="1649140018109";TID="8676089562051127148";CID="20224562657432"'
        },
        dataflow_default_options={
            "tempLocation": DATAFLOW_TEMP_LOCATION,
            "numWorkers": 2,
            "maxWorkers": 3,
            "machineType": "n1-standard-2",
            "subnetwork": DATAFLOW_SUBNETWORK,
            "usePublicIps": False
        }
    )

    job = operator.execute(dict())
    print(job)


with DAG(dag_id='bartek_dag',
         # start_date=datetime(2022, 8, 8),  # dag or task start_date is required,
         # schedule_interval defaults to execute once a day, catches up all runs since start date until today

         schedule_interval=timedelta(minutes=3),
         start_date=pendulum.utcnow().subtract(minutes=3),  # optionally specify end date
         end_date=pendulum.utcnow(),

         # schedule_interval="*/10 * * * *",
         # assigned when deployed from terraform: formatdate("YYYY-MM-DD'T'hh:mm:ss", timeadd(timestamp(),"-10m"))
         # start_date=pendulum.parse("2022-08-05T12:43:55", tz='UTC'),
         # assigned when deployed from terraform: formatdate("YYYY-MM-DD'T'hh:mm:ss", timestamp())
         # end_date=pendulum.parse("2022-08-05T12:53:55", tz='UTC'),

         # optional below
         default_args={
             'email': [EMAIL],
             'email_on_failure': True
         },
         description='Bartek dag description',
         tags=["bartek"],
         ) as dag:
    t1 = BashOperator(task_id='hello_bash_operator', bash_command='echo "Bartek" from bash')

    t2 = PythonOperator(task_id='hello_python_operator', python_callable=my_python_operator, op_args=["Bartek"],
                        op_kwargs={'city': 'NY'})

    t3 = PythonOperator(task_id='hello_python_with_context', python_callable=my_python_operator, op_args=["Bartek"],
                        op_kwargs={'city': 'NY'}, provide_context=True)

    t4 = GCSListObjectsOperator(task_id='gcs_list_operator', bucket=BUCKET, prefix='', delimiter=".*")

    t5 = PythonOperator(task_id='my_python_gcs_list_operator', python_callable=my_python_gcs_list_operator)

    t6 = PythonOperator(task_id='my_python_gcs_list_bash_operator_gsutil',
                        python_callable=my_python_gcs_list_bash_operator_gsutil)

    t7 = BashOperator(task_id='list_gcs_bash_operator_gsutil', bash_command=f"gsutil -i {SERVICE_ACCOUNT} ls {BUCKET}")

    t8 = PythonOperator(task_id='my_python_bq_select_count_client', python_callable=my_python_bq_select_count_client)

    t9 = PythonOperator(task_id='my_python_secret_manager_access_client',
                        python_callable=my_python_secret_manager_access_client)

    t10 = PythonOperator(task_id='my_python_dataflow_templated_operator',
                         python_callable=my_python_dataflow_templated_operator)
    # t1 >> t2
    # t1 >> t3
    # t1 >> t4
    # t1 >> t5
    # t1 >> t6
    # t1 >> t7
