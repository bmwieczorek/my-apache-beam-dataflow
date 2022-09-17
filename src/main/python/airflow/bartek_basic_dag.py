import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# pip install apache-airflow==1.10.15

with DAG(dag_id='bartek_basic_dag',
         schedule_interval="30 10 16 * *",
         start_date=pendulum.parse('2022-08-16T10:30:00', tz='UTC'),
         # schedule_interval="_SCHEDULE_INTERVAL_",
         # start_date=pendulum.parse('_START_DATE_', tz='UTC'),

         # optional below
         default_args={},
         description='Bartek basic dag description',
         tags=["bartek"],
         ) as dag:
    t1 = BashOperator(task_id='hello_bash_operator', bash_command='echo "Bartek" from bash')


# echo "Current local date time: $(date '+%Y-%m-%dT%H:%M:%S%z')" && \
# echo "Current UTC   date time: $(date -u '+%Y-%m-%dT%H:%M:%S%z')" && \
# cat bartek_basic_dag.py | \
# sed "s/_SCHEDULE_INTERVAL_/$(date -u -v +5M '+%M %H %d') * */" | \
# sed "s/_START_DATE_/$(date -u -v -1m -v +5M +%Y-%m-%dT%H:%M:00)/"
