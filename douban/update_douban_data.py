import inspect
import time
import os
from datetime import date
from imdb_mapping_douban_id import multi_thread_imdb_mapping_douban_id, get_imdb_id_list_douban_id_is_null
from scrapying_douban_detail_page_and_save_img import multi_thread_scrape_douban_detail_page_and_save_img
from clean_douban_detail_page import multi_thread_clean_douban_02_detail_page_and_save
from package.db.sql import (
    update_on_duplicate_key,
    save_processing_finished_log,
    MySQL)

file_name = os.path.basename(__file__)


def douban_id_total_count(sql_conn):
    sql = MySQL()
    table = """(SELECT movie_info.imdb_id, movie_info.start_year, movie_id_mapping.douban_id 
    FROM movie.movie_info
    LEFT JOIN movie.movie_id_mapping
    ON movie.movie_info.imdb_id = movie.movie_id_mapping.imdb_id) as t1"""
    condition = "WHERE start_year > 2017 AND douban_id is NOT NULL AND douban_id != 0"
    douban_id_mapping_imdb_count = sql.fetch_data(conn=sql_conn, table=table, columns=['douban_id'], struct='list', condition=condition)
    return len(douban_id_mapping_imdb_count)


def update_new_douban_data():
    start = time.time()
    sql = MySQL()
    engine, sql_conn = sql.get_connection()

    old_douban_id_total_count = douban_id_total_count(sql_conn)

    print('douban 01')
    multi_thread_imdb_mapping_douban_id(worker_num=10)
    print('douban 02')
    multi_thread_scrape_douban_detail_page_and_save_img(crawler_worker_num=25, cookies_worker_num=1,
                                                        target_docs_num=10)
    print('douban 03')
    multi_thread_clean_douban_02_detail_page_and_save(worker_num=25)

    end = time.time()

    # recheck result
    second_sql_conn = engine.connect()  # avoid mysql cache
    new_douban_id_is_null_imdb_id_list = get_imdb_id_list_douban_id_is_null()

    new_douban_id_total_count = douban_id_total_count(second_sql_conn)

    finished_num = new_douban_id_total_count - old_douban_id_total_count

    if len(new_douban_id_is_null_imdb_id_list) == 0:
        status_code = 1
    else:
        status_code = 2

    # insert log data
    log_info = {
        'connection': second_sql_conn,
        'start': start,
        'file_name': file_name,
        'func_name': inspect.stack()[0][3],
        'original_count': old_douban_id_total_count,
        'status_code': status_code
    }
    save_processing_finished_log(finished_num=finished_num, log_info=log_info)

    # update dashboard data

    dashboard_log = {
        'log_time': date.fromtimestamp(start),
        'douban_old_count': old_douban_id_total_count,
        'douban_id_count': new_douban_id_total_count,
        'douban_new_count': finished_num,
        'douban_data_time': round((end - start), 0)
    }
    update_on_duplicate_key(second_sql_conn, 'dashboard_movie_count', [dashboard_log], True)

    engine.dispose()


update_new_douban_data()