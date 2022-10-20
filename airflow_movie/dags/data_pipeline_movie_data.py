import pendulum

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Local application imports
from get_movie_data.imdb.update_imdb_data import update_new_imdb_data
from get_movie_data.douban.update_douban_data import update_new_douban_data
from get_movie_data.rotten_tomotoes.update_rotten_tomatoes_id import multi_thread_rotten_tomatoes_id


default_args = {
    'owner': 'Sing',
    'depends_on_past': False,
    'retries': 1,
    'email': ['newro9333@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retry_delay': timedelta(minutes=5)
}

today_date = datetime.today().date()

with DAG('update_movie_data',
         description='update movie data of IMDb & douban, and mapping rotten tomato id.',
         schedule_interval="@daily",
         start_date=pendulum.datetime(2022, 10, 1, tz="Asia/Taipei"),
         catchup=False) as dag:

    update_new_imdb_movie = PythonOperator(
        task_id="update_new_imdb_data",
        python_callable=update_new_imdb_data,
    )

    update_new_douban_movie = PythonOperator(
        task_id="update_douban_data",
        python_callable=update_new_douban_data,
    )

    update_new_rotten_tomatoes_id = PythonOperator(
        task_id="update_tomato_data",
        python_callable=multi_thread_rotten_tomatoes_id,
        op_kwargs={'worker_num': 22, 'year': 2017},
    )

    update_new_imdb_movie >> update_new_douban_movie >> update_new_rotten_tomatoes_id
