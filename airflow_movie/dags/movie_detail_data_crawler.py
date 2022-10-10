import sys
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from imdb.update_imdb_data import update_new_imdb_data

from douban.update_douban_data import update_new_douban_data
from update_rating.update_rotten_tomatoes_id import multi_thread_rotten_tomatoes_id

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

with DAG('update_movie_data',
         description='update movie data of IMDb & douban, and mapping rotten tomato id.',
         schedule_interval="@daily",
         start_date=pendulum.datetime(2022, 10, 1, tz="Asia/Taipei"),
         catchup=False) as dag:

    update_new_imdb_data = PythonOperator(
        task_id="update_new_imdb_data",
        python_callable=update_new_imdb_data,
    )

    update_new_douban_data = PythonOperator(
        task_id="update_douban_data",
        python_callable=update_new_douban_data
    )

    update_new_rotten_tomatoes_id = PythonOperator(
        task_id="update_douban_data",
        python_callable=multi_thread_rotten_tomatoes_id(worker_num=20, year=2017),
        op_kwargs={'worker_num': 1, 'year': 2007},
    )

    update_new_imdb_data >> update_new_douban_data >> update_new_rotten_tomatoes_id
