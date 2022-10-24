import inspect
import os
import time

from time import sleep
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Local application imports
from local_package.db.mysql import save_error_log, MySQL, update_on_duplicate_key
from local_package.web_crawler_tools.multi_thread import SeleniumCrawler


file_name = os.path.basename(__file__)


def get_douban_id(url_str):
    clean_id = url_str.split('subject/')[1].rstrip('/').strip()
    return clean_id


def imdb_mapping_douban_id(imdb_id, sql_conn, driver):
    # mysql table 'movie.processing_finished_log'
    log_info = {
        'connection': sql_conn,
        'start': time.time(),
        'file_name': file_name,
        'func_name': inspect.stack()[0][3],
    }

    # driver the page
    url = "https://search.douban.com/movie/subject_search?search_text=" + imdb_id
    try:
        driver.get(url)
    except Exception as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        driver.quit()
        return 2

    # loading page, if failed then try 3 times
    def refresh(try_num):
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'footer'))
            )
            return True

        except Exception as msg:
            try_num += 1
            if try_num < 3:
                driver.refresh()
                refresh(try_num)
                sleep(2)
            else:
                msg_text = str(msg) + ' loading element failed after try {} times.'.format(str(try_num))
                save_error_log(status=2, imdb_id=imdb_id, msg=msg_text, log_info=log_info)
                driver.quit()
                return False

    first_try_num = 0
    loading_element_successfully = refresh(first_try_num)

    if not loading_element_successfully:
        return 2

    # anti-scraping
    if driver.title == '豆瓣 - 登录跳转页':
        driver.quit()
        return 4

    # find result element
    try:
        result = driver.find_element(By.CLASS_NAME, 'sc-gqjmRU').text
        if '没有找到' in result:
            douban_row = {'imdb_id': imdb_id, 'douban_id': 0}
            update_on_duplicate_key(sql_conn, 'movie_id_mapping', [douban_row], multi_thread=True)
            driver.quit()
            return douban_row

    except Exception as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        driver.quit()
        return 2
    try:
        # element: sc-bZQynM > item-root > a >href
        result_list = driver.find_elements(By.CLASS_NAME, 'sc-bZQynM')
        first_card = result_list[0]
        a_tag = first_card.find_element(By.CLASS_NAME, 'item-root').find_element(By.TAG_NAME, 'a')
        match_result_url = a_tag.get_attribute('href')
        douban_id = get_douban_id(match_result_url)
        douban_row = {'imdb_id': imdb_id, 'douban_id': douban_id}
        update_on_duplicate_key(sql_conn, 'movie_id_mapping', [douban_row], multi_thread=True)
        driver.quit()
        return douban_row

    except Exception as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        driver.quit()
        return 2


def get_imdb_id_list_douban_id_is_null():
    sql = MySQL()
    engine, connection = sql.get_connection()
    query = """SELECT imdb_id from
    (SELECT movie_info.imdb_id, movie_info.start_year, movie_id_mapping.douban_id FROM movie.movie_info
    INNER JOIN movie.movie_id_mapping
    ON movie.movie_info.imdb_id = movie.movie_id_mapping.imdb_id) as t1
    WHERE start_year > 2017 and douban_id is null;"""
    imdb_tuple_list = connection.execute(query).fetchall()
    imdb_list = [my_tuple[0]for my_tuple in imdb_tuple_list]

    connection.close()
    engine.dispose()
    return imdb_list


def multi_thread_imdb_mapping_douban_id(worker_num):
    function_resource_args_list = ['sql_conn', 'driver']
    imdb_list = get_imdb_id_list_douban_id_is_null()
    multi_thread_cookies_creator = SeleniumCrawler(
        id_list=imdb_list,
        worker_num=worker_num,
        func=imdb_mapping_douban_id,
        file_name=file_name,
        table='movie_id_mapping',
        resource_list=function_resource_args_list)
    multi_thread_cookies_creator.create_worker()
