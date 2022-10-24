import requests
import gzip
import re
import time
import os
import inspect
import pathlib

from datetime import date

# Local application imports
from local_package.db.mysql import (
    update_on_duplicate_key,
    save_processing_finished_log,
    MySQL)

# SAVE_PATH = '/home/ec2-user/airflow_temp/imdb/'
SAVE_PATH = pathlib.Path(__file__).parent.absolute()
file_name = os.path.basename(__file__)


def download_imdb_dataset():
    # title.basics.tsv.gz
    url = 'https://datasets.imdbws.com/title.basics.tsv.gz'
    filename = url.split("/")[-1]
    with open(SAVE_PATH / filename, "wb") as f:
        r = requests.get(url)
        f.write(r.content)

    # title.akas.tsv.gz
    url = 'https://datasets.imdbws.com/title.akas.tsv.gz'
    filename = url.split("/")[-1]
    with open(SAVE_PATH / filename, "wb") as f:
        r = requests.get(url)
        f.write(r.content)

    # title.principals.tsv.gz
    url = 'https://datasets.imdbws.com/title.principals.tsv.gz'
    filename = url.split("/")[-1]
    with open(SAVE_PATH / filename, "wb") as f:
        r = requests.get(url)
        f.write(r.content)

    # name.basics.tsv.gz
    url = 'https://datasets.imdbws.com/name.basics.tsv.gz'
    filename = url.split("/")[-1]
    with open(SAVE_PATH / filename, "wb") as f:
        r = requests.get(url)
        f.write(r.content)


def delete_imdb_dataset():
    os.unlink(SAVE_PATH / 'title.basics.tsv.gz')
    os.unlink(SAVE_PATH / 'title.akas.tsv.gz')
    os.unlink(SAVE_PATH / 'title.principals.tsv.gz')
    os.unlink(SAVE_PATH / 'name.basics.tsv.gz')


# title.basics.tsv.gz
def update_new_imdb_id_and_movie_info(old_imdb_id_list):
    min_year = 2017  # web movie year range
    sql = MySQL()
    engine, sql_conn = sql.get_connection()

    # create old_imdb_id_list hash table
    old_imdb_id_hash = {imdb_id: 1 for imdb_id in old_imdb_id_list}

    # get movie genre type list
    genre_table = sql.fetch_data(conn=sql_conn, table='genre_type', columns=['genre_id', 'genre'], struct='dict')
    genre_id_hash = {row['genre']: row['genre_id'] for row in genre_table}

    # read unzip file, then convert binary to string
    basic_info_insert_list = []
    genre_insert_list = []
    new_movie_id_list = []

    new_movie_count = 0
    with gzip.open(SAVE_PATH / 'title.basics.tsv.gz', "rb") as unzip_file:
        next(unzip_file)
        for line in unzip_file:
            content_list = line.decode('utf-8').rstrip('\n').split('\t')
            # content_list ['tt0018479', 'movie', 'A Texas Steer', 'A Texas Steer', '0', '1927', '\\N', '80', 'Comedy']

            if re.match(r'\\N', content_list[5]) is None:
                start_year = int(content_list[5])
                imdb_id = content_list[0]
                id_type = content_list[1]

                # find new movie need to insert into database
                if id_type == 'movie' and imdb_id not in old_imdb_id_hash and start_year > min_year:

                    for idx, col in enumerate(content_list):
                        if re.match(r'\\N', col) is not None:
                            content_list[idx] = None

                    new_movie_count = new_movie_count + 1
                    # cleaning e.g. 'endYear': '\\N' by regular

                    movie_data_dict = {
                        'imdb_id': imdb_id,
                        'primary_title': content_list[2],
                        'original_title': content_list[3],
                        'is_adult': content_list[4],
                        'start_year': start_year,
                        'runtime': content_list[7]
                    }
                    basic_info_insert_list.append(movie_data_dict)
                    new_movie_id_list.append(imdb_id)

                    # genre
                    # if re.match(r'\\N', content_list[8]) is None:
                    if content_list[8] is not None:
                        genre_list = content_list[8].split(',')
                        for genre in genre_list:
                            genre_dict = {
                                'imdb_id': content_list[0],
                                'genre_id': genre_id_hash[genre]
                            }
                            genre_insert_list.append(genre_dict)
    new_movie_id_dict_list = [{'imdb_id': i} for i in new_movie_id_list]

    # data list is not empty, new movie need to update
    if basic_info_insert_list:
        update_on_duplicate_key(sql_conn, 'movie_info', basic_info_insert_list, True)
    if genre_insert_list:
        update_on_duplicate_key(sql_conn, 'movie_genre', genre_insert_list, True)
    if new_movie_id_dict_list:
        update_on_duplicate_key(sql_conn, 'movie.movie_id_mapping', new_movie_id_dict_list, True)
    sql_conn.close()
    engine.dispose()
    return new_movie_id_list


# title.akas.tsv.gz
def save_tw_name_into_movie_info():
    sql = MySQL()
    engine, sql_conn = sql.get_connection()
    # get movie imdb_id list
    movie_id_list = sql.fetch_data(conn=sql_conn,
                                   table='movie_info',
                                   columns=['imdb_id'],
                                   condition='WHERE tw_name is null',
                                   struct='list')
    movie_id_hash = {movie_id: 1 for movie_id in movie_id_list}

    # read unzip file, then convert binary to string
    movie_count = 0
    data_list = []
    with gzip.open(SAVE_PATH / 'title.akas.tsv.gz', "rb") as unzip_file:
        for idx, line in enumerate(unzip_file):
            content_list = line.decode('utf-8').rstrip('\n').split('\t')

            if content_list[0] in movie_id_hash and content_list[3] == 'TW':  # id type is movie and region=TW
                movie_count = movie_count + 1

                movie_data_dict = {
                    'imdb_id': content_list[0],
                    'tw_name': content_list[2]
                }
                data_list.append(movie_data_dict)
        if data_list:
            update_on_duplicate_key(sql_conn, 'movie_info', data_list)

    sql_conn.close()
    engine.dispose()


# used by "save_staff_into_movie_info()"
def save_celebrity_into_movie_info(staff_person_id_list):
    sql = MySQL()
    engine, sql_conn = sql.get_connection()

    # get movie imdb_id list
    staff_person_id_hash = {movie_id: 1 for movie_id in staff_person_id_list}

    # read unzip file, then convert binary to string
    celebrity_data_list = []
    inserted_imdb_person_id_list = []
    count_movie = 0

    with gzip.open(SAVE_PATH / 'name.basics.tsv.gz', "rb") as unzip_file:
        for idx, line in enumerate(unzip_file):
            content_list = line.decode('utf-8').rstrip('\n').split('\t')
            # content_list ['nm0000001', 'Fred Astaire', '1899', '1987', 'soundtrack,actor,miscellaneous', 'tt0050419,tt0031983']

            if content_list[0] in staff_person_id_hash:  # person_id in staff table
                count_movie = count_movie + 1
                if idx > 0:  # pass header
                    celebrity_dict = {
                        'imdb_per': content_list[0],
                        'en_name': content_list[1]
                    }
                    celebrity_data_list.append(celebrity_dict)
                    inserted_imdb_person_id_list.append(content_list[0])

        # insert celebrity
        if celebrity_data_list:
            update_on_duplicate_key(sql_conn, 'celebrity', celebrity_data_list, True)

        with open(SAVE_PATH / 'lack_imdb_person_id_in_celebrity.txt', 'a') as f:
            for person_id in staff_person_id_hash:
                if person_id not in {i: 1 for i in inserted_imdb_person_id_list}:
                    f.write('person_id: ' + person_id + '\n')
    sql_conn.close()
    engine.dispose()
    return inserted_imdb_person_id_list


# title.principals.tsv.gz
def save_staff_into_movie_info():
    sql = MySQL()
    engine, sql_conn = sql.get_connection()

    # get movie imdb_id list
    condition = """
    LEFT JOIN  movie.staff 
    ON movie.movie_info.imdb_id = movie.staff.imdb_id
    WHERE imdb_per IS NULL AND start_year > 2017;"""
    staff_is_null_movie_id_list = sql.fetch_data(conn=sql_conn,
                                                 table='movie_info',
                                                 columns=['DISTINCT imdb_id'],
                                                 condition=condition,
                                                 struct='list')
    movie_id_hash = {movie_id: 1 for movie_id in staff_is_null_movie_id_list}

    # read unzip file, then convert binary to string
    staff_data_list = []
    staff_person_id_set = set()
    safe_staff_data_list = []  # recheck by func 'save_celebrity_into_movie_info()'
    count_movie = 0

    with gzip.open(SAVE_PATH / 'title.principals.tsv.gz', "rb") as unzip_file:
        next(unzip_file)
        for idx, line in enumerate(unzip_file):
            content_list = line.decode('utf-8').rstrip('\n').split('\t')
            # content_list ['tt0003127', '1', 'nm0298301', 'actress', '\\N', '["Mary Queen of Scots"]']
            if content_list[0] in movie_id_hash:  # id_type is 'movie'
                person_id = content_list[2]
                count_movie = count_movie + 1

                job_type = content_list[3]
                if job_type == 'self' and re.match(r'\\N', content_list[5]) is None:  # characters not '\\N'
                    job_type = 'actor'

                staff_dict = {
                    'imdb_id': content_list[0],
                    'imdb_per': content_list[2],
                    'job_type': job_type
                }
                staff_person_id_set.add(person_id)
                staff_data_list.append(staff_dict)

        # add new celebrity person_id
        if 1:
            inserted_imdb_person_id_list = save_celebrity_into_movie_info(list(staff_person_id_set))
            inserted_imdb_person_id_list_hash = {i: 1 for i in inserted_imdb_person_id_list}
        # insert the last staff_data_list
        if staff_data_list:
            for staff_dict in staff_data_list:
                person_id = staff_dict['imdb_per']
                if person_id in inserted_imdb_person_id_list_hash:
                    safe_staff_data_list.append(staff_dict)

        if safe_staff_data_list:
            update_on_duplicate_key(sql_conn, 'staff', safe_staff_data_list, True)

    sql_conn.close()
    engine.dispose()
    return staff_is_null_movie_id_list, staff_data_list


# merge function
def update_new_imdb_data():
    start = time.time()
    sql = MySQL()
    engine, sql_conn = sql.get_connection()

    # get existed imdb_id list from database
    min_year = 2017
    condition = 'WHERE start_year > {}'.format(min_year)
    old_imdb_id_list = sql.fetch_data(conn=sql_conn,
                                      table='movie_info',
                                      columns=['imdb_id'],
                                      struct='list',
                                      condition=condition)

    download_imdb_dataset()

    # update new imdb_id & basic info
    new_add_movie_id_list = update_new_imdb_id_and_movie_info(old_imdb_id_list)

    # update column tw_name in table movie info
    save_tw_name_into_movie_info()

    # update table staff & celebrity
    save_staff_into_movie_info()

    end = time.time()

    # recheck result
    sql_conn.close()
    second_sql_conn = engine.connect()  # avoid mysql cache
    new_imdb_id_list = sql.fetch_data(conn=second_sql_conn,
                                      table='movie_info',
                                      columns=['imdb_id', 'start_year'],
                                      struct='list',
                                      condition=condition)

    finished_num = len(new_imdb_id_list) - len(old_imdb_id_list)

    if len(new_imdb_id_list) == len(old_imdb_id_list) + len(new_add_movie_id_list):
        status_code = 1
    else:
        status_code = 2

    # insert log data
    log_info = {
        'connection': second_sql_conn,
        'start': start,
        'file_name': file_name,
        'func_name': inspect.stack()[0][3],
        'original_count': len(old_imdb_id_list),
        'status_code': status_code
    }

    dashboard_log = {
        'log_time': date.fromtimestamp(start),
        'imdb_old_count':  len(old_imdb_id_list),
        'imdb_id_count': len(new_imdb_id_list),
        'imdb_new_count': len(new_add_movie_id_list),
        'imdb_data_time': round(end-start, 0)
    }
    save_processing_finished_log(finished_num=finished_num, log_info=log_info)
    update_on_duplicate_key(second_sql_conn, 'dashboard_movie_count', [dashboard_log], True)
    delete_imdb_dataset()
    engine.dispose()
