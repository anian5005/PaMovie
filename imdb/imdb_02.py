import requests
from bs4 import BeautifulSoup
import json
from package.db.mongo import insert_mongo_doc, create_mongo_connection
from package.db.sql import save_error_log, save_processing_log, create_conn_pool, get_connection, sql_select_column, mark_column
import os
import inspect
from package.general import get_current_taiwan_datetime
from package.multi_thread import Multi_thread


def get_scraping_imdb_id(year=None):
    engine = create_conn_pool()
    sql_conn = get_connection(engine)
    base_condition = " WHERE type = '{}' AND imdb_crawler <=> Null".format('Movie')
    if year != None:
        year_condition = ' AND year = {year}'.format(year=year)
        condition = base_condition + year_condition
        print(condition)
    else:
        condition = base_condition
    id_list = sql_select_column(table='imdb_movie_id', columns=['imdb_id'], connection=sql_conn, struct='list', condition=condition)
    sql_conn.close()
    engine.dispose()
    return id_list


# status_code: 2: error, 1: successful
def scrape_imdb_detail_page(sql_conn, imdb_id):
    os.path.join(os.path.dirname(__file__))
    mongo_db, mongo_conn = create_mongo_connection()
    file_name = os.path.basename(__file__)
    start = get_current_taiwan_datetime()
    func = inspect.stack()[0][3]
    url = 'https://www.imdb.com/title/' + imdb_id

    try:
        response = requests.get(url, headers={'accept-language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7'})
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func, status=2, imdb_id=imdb_id, msg=er)
        mark_column(connection=sql_conn, table_name='imdb_movie_id', column_name='imdb_crawler', status=2,
                    where_id='imdb_id', id_value=imdb_id)
        return 2
    # basic info
    try:
        str_data = soup.find('script', {'type': 'application/ld+json'}).text
        json_data = json.loads(str_data)
    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func, status=2, imdb_id=imdb_id, msg=er)
        mark_column(connection=sql_conn, table_name='imdb_movie_id', column_name='imdb_crawler', status=2,
                    where_id='imdb_id', id_value=imdb_id)
        # error_status_code:2
        return 2
    # actor list
    try:
        cast_div = soup.find('div', {'class': 'title-cast__grid'})
        cast_list = cast_div.findAll('a', {'data-testid': 'title-cast-item__actor'})
        cast_str_list = [str(soup_obj) for soup_obj in cast_list]
    except:
        cast_str_list = []
    date_ymd = get_current_taiwan_datetime().strftime('%Y-%m-%d')

    # merge data
    data = {
        'imdb_id': imdb_id,
        "created_date": date_ymd,
        'json_data': json_data,
        'cast_list': cast_str_list
    }
    # save data into mongo
    try:
        insert_mongo_doc(db=mongo_db, collection_name='imdb_02_raw_detail_page', doc=data)
        save_processing_log(connection=sql_conn, start=start, file_name=file_name, func_name=func, status=1, imdb_id=imdb_id)
        mongo_conn.close()
        mark_column(connection=sql_conn, table_name='imdb_movie_id', column_name='imdb_crawler', status=1, where_id='imdb_id', id_value=imdb_id)
        return 1
    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func, status=3, imdb_id=imdb_id, msg=er)
        return 2


def multi_thead_crawler_imdb_detail_page(year, worker_num):
    file_name = os.path.basename(__file__)
    imdb_list = get_scraping_imdb_id(year=year)
    print('job num:', len(imdb_list))
    imdb_detail_crawler = Multi_thread(imdb_list, worker_num,  scrape_imdb_detail_page, file_name)
    imdb_detail_crawler.create_worker()

# scrape all imdb_id where imdb_crawler is null
# for i in range(1894, 1895):
#     print('year', i)
# multi_thead_crawler_imdb_detail_page(year=None, worker_num=20)