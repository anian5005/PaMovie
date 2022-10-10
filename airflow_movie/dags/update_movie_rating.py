import sys
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from update_rating.update_movie_rating import multi_thread_update_rating

PATH = '/home/ec2-user/airflow_temp'
sys.path.insert(0, PATH)

default_args = {
    'owner': 'Sing',
    'depends_on_past': False,
    'retries': 1,
    'email': ['newro9333@gmail.com'],
    'email_on_failure': True,
    'retry_delay': timedelta(minutes=5)
}

today_date = datetime.today().date()

with DAG('update_movie_ratings',
         description='update movie ratings of IMDb & douban & rotten tomatoes.',
         schedule_interval="@daily",
         start_date=pendulum.datetime(2022, 10, 1, tz="Asia/Taipei"),
         catchup=False) as dag:

    multi_thread_update_rating = PythonOperator(
        task_id="multi_thread_update_rating",
        python_callable=multi_thread_update_rating,
    )