from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Local application imports
from get_movie_data.get_ratings.update_movie_rating import (
    update_imdb_ratings,
    multi_thread_update_douban_ratings,
    multi_thread_update_rotten_tomatoes_ratings)


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

with DAG('update_movie_ratings',
         description='update movie ratings of IMDb & douban & rotten tomatoes.',
         schedule_interval="@daily",
         start_date=pendulum.datetime(2022, 10, 1, tz="Asia/Taipei"),
         catchup=False) as dag:

    download_dataset_and_update_imdb_ratings = PythonOperator(
        task_id="update_imdb_ratings",
        python_callable=update_imdb_ratings,
    )

    update_douban_ratings = PythonOperator(
        task_id="update_douban_ratings",
        python_callable=multi_thread_update_douban_ratings,
        op_kwargs={'worker_num': 25,  'target_docs_num': 40},
    )

    update_rotten_tomatoes_ratings = PythonOperator(
        task_id="update_rotten_tomatoes_ratings",
        python_callable=multi_thread_update_rotten_tomatoes_ratings,
        op_kwargs={'worker_num': 20},
    )

    download_dataset_and_update_imdb_ratings >> update_rotten_tomatoes_ratings >> update_douban_ratings
