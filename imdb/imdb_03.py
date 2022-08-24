from bs4 import BeautifulSoup
import re
import os
import inspect
from package.db.mongo import get_mongo_doc_by_date, create_mongo_connection, get_mongo_doc_by_id
from package.db.sql import insert_dict_list_into_sql, sql_select_column, insert_dict_into_sql, save_error_log, save_processing_log, mark_column, create_conn_pool, get_connection
from package.multi_thread import Multi_thread
from package.general import get_current_taiwan_datetime

fname = os.path.basename(__file__)


def get_clean_imdb_id(year=None):
    engine = create_conn_pool()
    sql_conn = get_connection(engine)
    base_condition = " WHERE type = '{}' AND imdb_clean <=> Null AND imdb_crawler =1".format('Movie')
    if year is not None:
        year_condition = ' AND year = {year}'.format(year=year)
        condition = base_condition + year_condition
    else:
        condition = base_condition
    id_list = sql_select_column(table='imdb_movie_id', columns=['imdb_id'], connection=sql_conn, struct='list', condition=condition)
    sql_conn.close()
    engine.dispose()
    return id_list


def get_imdb_per_id(url_str):
    url_str = url_str.replace('/name/', '')
    imdb_per_id = url_str.split('?')[0].replace('/', '')
    return imdb_per_id


# input: PT1H10M, PT1M
def get_imdb_duration(time_str):
    hour = 0
    minute = 0
    time_str.replace('PT', '')
    if re.search(r'\d+H', time_str) is not None:
        hour_str = re.search(r'(\d+)H', time_str).group(1)
        hour = hour_str.split('H')[0]
    if re.search(r'\d+M', time_str) is not None:
        minute_str = re.search(r'(\d+)M', time_str).group(1)
        minute = minute_str.split('M')[0]
    hour = int(hour)
    minute = int(minute)
    duration = hour*60 + minute
    return duration


def imdb_03_cleaning(sql_conn, doc):
    start = get_current_taiwan_datetime()
    func = inspect.stack()[0][3]
    imdb_id = doc['imdb_id']
    # check main data exist
    try:
        json_data = doc['json_data']
    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=fname, func_name=func, status=2, imdb_id=imdb_id,
                       msg=er)
        mark_column(connection=sql_conn, table_name='imdb_movie_id', column_name='imdb_clean', status=2,
                    where_id='imdb_id', id_value=imdb_id)
        return 2

    # genre_type not exist in db table genre_type, then insert new data
    movie_genre_dict_list = []
    try:
        if 'genre' in json_data:
            genres = json_data['genre']
            # get table: genre mapping genre_id
            # [{'genre_id': 1, 'genre': 'Action'}, {'genre_id': 2, 'genre': 'Drama'}]
            genre_db_table = sql_select_column(connection=sql_conn, table='genre_type', columns=['genre_id', 'genre'],  struct='dict')
            genre_db_hash_table = {row['genre']: row['genre_id'] for row in genre_db_table}

            for genre in genres:
                if genre in genre_db_hash_table:
                    pass
                else:
                    insert_dict_into_sql(connection=sql_conn, table_name='genre_type', my_dict={'genre': genre}, ignore=None)
            for genre in genres:
                movie_genre_dict_list.append({'imdb_id': imdb_id, 'genre_id': genre_db_hash_table[genre]})

    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=fname, func_name=func, status=2, imdb_id=imdb_id, msg=er)

    # plot
    plot = {}
    try:
        if 'description' in json_data:
            plot = json_data['description']
            plot = {'imdb_id': imdb_id, 'en_plot': plot}
    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=fname, func_name=func, status=2, imdb_id=imdb_id,
                       msg=er)

    # get movie info from json_data
    info = {}
    columns = {'alternateName': 'tw_name', 'name': 'en_name', 'datePublished': 'published', 'duration': 'runtime'}
    for key in json_data:
        if key in columns:
            if key == 'duration':
                duration = get_imdb_duration(json_data[key])  # e.g. "PT2H8M" -> {'runtime': 128}
                info.update({columns[key]: duration})
            else:
                info.update({columns[key]: json_data[key]})  # e.g. {"tw_name": "捍衛戰士：獨行俠"}

    # get directors & creators from json_data
    staff_list = []
    celebrity_list = []
    raw_type_mapping_column_type = {'director': 'director', 'creator': 'writer'}
    for type_name in raw_type_mapping_column_type:
        if type_name in json_data:
            person_list = json_data[type_name]
            for per in person_list:
                if per['@type'] == "Person":
                    imdb_per_id = get_imdb_per_id(per['url'])
                    per_name = per['name']
                    staff_list.append({'imdb_movie': imdb_id, 'job_type': raw_type_mapping_column_type[type_name], 'imdb_per': imdb_per_id})
                    celebrity_list.append({'imdb_per': imdb_per_id, 'en_name': per_name})

    # get actor from cast_soup_list
    try:
        cast_soup_list = [BeautifulSoup(i, 'lxml') for i in doc['cast_list']]
        for soup in cast_soup_list:
            a_tag = soup.find('a', {'data-testid': 'title-cast-item__actor'})
            actor_name = a_tag.text.replace('\n', '').strip()
            imdb_per_id = get_imdb_per_id(a_tag['href'])
            celebrity_list.append({'imdb_per': imdb_per_id, 'en_name': actor_name})
            staff_list.append({'imdb_movie': imdb_id, 'job_type': 'actor', 'imdb_per': imdb_per_id})
    except Exception as er:
        save_error_log(connection=sql_conn, file_name=fname, start=start, func_name=func, status=2, imdb_id=imdb_id,
                       msg=er)

    # safe insert
    table_mapping_data = {
        'celebrity': celebrity_list,
        'movie_genre': movie_genre_dict_list,
        'staff': staff_list,
        'plot': plot,
        'movie_info': info
    }
    safe_insert_table = []
    for key in table_mapping_data:
        if table_mapping_data[key] != [] or table_mapping_data[key] != {}:
            if key == 'movie_info':
                info.update({'imdb_id': imdb_id})
            safe_insert_table.append(key)
    try:
        for table in safe_insert_table:
            data = table_mapping_data[table]
            if isinstance(data, dict):
                insert_dict_into_sql(connection=sql_conn, table_name=table, my_dict=data, ignore=True)
            if isinstance(data, list):
                insert_dict_list_into_sql(connection=sql_conn, table_name=table, dict_list=data, ignore=True)
        # save successful log
        save_processing_log(connection=sql_conn, start=start, file_name=fname, func_name=func, status=1, imdb_id=imdb_id)
        mark_column(connection=sql_conn, table_name='imdb_movie_id', column_name='imdb_clean', status=1,
                    where_id='imdb_id', id_value=imdb_id)
        return 1
    except Exception as er:
        save_error_log(connection=sql_conn, file_name=fname, start=start, func_name=func, status=3, imdb_id=imdb_id, msg=er)
        return 2


def multi_thread_cleaning_mongo_imdb_02_detail_page(start, end, worker_num):
    caller_file = os.path.basename(__file__)
    mongo_db, mongo_conn = create_mongo_connection()
    docs = get_mongo_doc_by_date(mongo_db, start, end, collection_name='imdb_02_raw_detail_page')
    imdb_detail_cleaner = Multi_thread(id_list=docs, worker_num=worker_num, func=imdb_03_cleaning, file_name=caller_file)
    imdb_detail_cleaner.create_worker()
    mongo_conn.close()


multi_thread_cleaning_mongo_imdb_02_detail_page(start="2022-08-13", end="2022-08-14", worker_num=20)


def test():
    # multi_thread_cleaning_mongo_imdb_02_detail_page(start='2022-08-12', end='2022-08-13', worker_num=1)
    engine = create_conn_pool()
    sql_conn = get_connection(engine)
    mongo_db, mongo_conn = create_mongo_connection()

    id_list = get_clean_imdb_id()
    for i in id_list:
        try:
            docs = get_mongo_doc_by_id(mongo_db, 'imdb_02_raw_detail_page', 'imdb_id', i)
            imdb_03_cleaning(sql_conn=sql_conn, doc=docs[0])
        except:
            pass
    mongo_conn.close()
    sql_conn.close()
    engine.dispose()
