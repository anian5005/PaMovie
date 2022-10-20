# Standard library imports
import requests
import json
import time
import os

# Third party imports
from datetime import date
from dotenv import load_dotenv

# Local application imports
from local_package.db.mysql import MySQL
from local_package.web_crawler_tools.multi_thread import MultiThread

load_dotenv()
OMDB_KEY = os.getenv('OMDB_KEY')

file_name = os.path.basename(__file__)


def get_omdb_api_data(imdb_id):
    url = "http://www.omdbapi.com/?i={}&plot=full".format(imdb_id) + OMDB_KEY
    response = requests.get(url)
    if response.status_code == 200:
        result_json = json.loads(response.text)
        if "Ratings" in result_json:
            return {'imdb_id': imdb_id, 'rating': result_json["Ratings"]}

    elif response.status_code == 401:  # Request limit reached
        return 'stop worker'  # stop worker

    return None


def update_rating_by_omdb_api(imdb_id, sql_conn):
    omdb_response = get_omdb_api_data(imdb_id)

    if omdb_response is not None:
        ratings = omdb_response['rating']
        # ratings [{"Source":"Rotten Tomatoes","Value":"96%"},{"Source":"Metacritic","Value":"78/100"}.]

        for item in ratings:
            source_name = item["Source"]
            rating_value = item["Value"]

            if source_name == "Rotten Tomatoes":
                rotten_tomatoes_value = rating_value.replace('%', '')
                tomato_dict = {
                    'imdb_id': imdb_id,
                    'tomatoes_rating': int(rotten_tomatoes_value),
                    'tomatoes_source': 'omdb',
                    'tomatoes_updated': date.fromtimestamp(time.time())
                }

                return tomato_dict
    else:
        return 1


def get_imdb_id_list_rotten_tomatoes_id_is_null(start_year):
    sql = MySQL()
    engine, connection = sql.get_connection()
    query = """SELECT imdb_id from 
    (SELECT movie_info.imdb_id, movie_info.start_year, movie_id_mapping.douban_id, movie_id_mapping.rotten_tomatoes_id 
    FROM movie.movie_info
    INNER JOIN movie.movie_id_mapping
    ON movie.movie_info.imdb_id = movie.movie_id_mapping.imdb_id) as t1
    WHERE start_year > 2017 AND douban_id IS NOT NULL AND douban_id != 0 AND 
    (rotten_tomatoes_id IS NULL OR rotten_tomatoes_id ='0') ;""".format(start_year)
    imdb_tuple_list = connection.execute(query).fetchall()
    imdb_list = [my_tuple[0]for my_tuple in imdb_tuple_list]

    connection.close()
    engine.dispose()
    return imdb_list


def multi_thread_update_tomato_rating_by_omdb_api(start_year):
    sql = MySQL()
    engine, sql_conn = sql.get_connection()

    imdb_id_list_rotten_tomatoes_id_is_null = [{'imdb_id': i} for i in get_imdb_id_list_rotten_tomatoes_id_is_null(start_year)]

    multi_thread_cookies_creator = MultiThread(id_list=imdb_id_list_rotten_tomatoes_id_is_null,
                                               worker_num=30,
                                               func=update_rating_by_omdb_api,
                                               file_name=file_name,
                                               table='movie_rating',
                                               resource_list=['sql_conn']
                                               )

    multi_thread_cookies_creator.create_worker()

    sql_conn.close()
    engine.dispose()
