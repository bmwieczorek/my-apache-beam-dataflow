from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


# pip install apache-airflow==1.10.15

with DAG(dag_id='bartek_dag',
         # start_date=datetime(2022, 8, 8),  # dag or task start_date is required,
         # schedule_interval defaults to execute once a day, catches up all runs since start date until today

         # schedule_interval=timedelta(minutes=3),
         # start_date=pendulum.utcnow().subtract(minutes=3),  # optionally specify end date
         # end_date=pendulum.utcnow(),

         schedule_interval="*/10 * * * *",
         start_date=pendulum.parse("2022-08-05T12:43:55", tz='UTC'),  # assigned when deployed from terraform: formatdate("YYYY-MM-DD'T'hh:mm:ss", timeadd(timestamp(),"-10m"))
         end_date=pendulum.parse("2022-08-05T12:53:55", tz='UTC'),  # assigned when deployed from terraform: formatdate("YYYY-MM-DD'T'hh:mm:ss", timestamp())

         # optional below
         default_args={
             'email': ['bartosz.wieczorek@sabre.com'],
             'email_on_failure': True},
         description='Bartek dag description',
         tags=["bartek"],
         ) as dag:
    t1 = BashOperator(task_id='print_hello', bash_command='echo "Bartek"')
