from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from package.db.sql import insert_dict_into_sql
from time import sleep
import inspect
import os
from package.db.sql import save_error_log, create_conn_pool, get_connection, mark_column
from package.general import get_current_taiwan_datetime
from package.multi_thread import Multi_thread_selenium_crawler


def get_douban_id(url_str):
    clean_id = url_str.split('subject/')[1].rstrip('/').strip()
    return clean_id


# status_code: 2: error, 0: no match imdb_id, 1: successful
def douban_01_by_id(connection, imdb_id, driver):
    file_name = 'douban_01.py'
    start = get_current_taiwan_datetime()
    func = inspect.stack()[0][3]
    print('imdb_id', imdb_id)

    # driver the page
    url = "https://search.douban.com/movie/subject_search?search_text=" + imdb_id
    try:
        sleep(5)
        driver.get(url)
    except Exception as er:
        save_error_log(connection=connection, start=start, file_name=file_name, func_name=func, status=2,
                       imdb_id=imdb_id, msg=str(er))
        return 2

    # loading page
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'sc-gqjmRU'))
        )
    except Exception as er:
        save_error_log(connection=connection, start=start, file_name=file_name, func_name=func, status=2,
                       imdb_id=imdb_id, msg=str(er))
        return 2
    # find result element

    try:
        result = driver.find_element(By.CLASS_NAME, 'sc-gqjmRU').text
        # print('sc-bZQynM result', result)
        if '没有找到' in result:
            mark_column(connection, 'imdb_movie_id', 'douban_01', 0, 'imdb_id', imdb_id)
            # driver.quit()
            return 0
    except Exception as er:
        save_error_log(connection=connection, start=start, file_name=file_name, func_name=func, status=2,
                       imdb_id=imdb_id, msg=str(er))
        return 2
    try:
        # sc-bZQynM > item-root > a >href
        result_list = driver.find_elements(By.CLASS_NAME, 'sc-bZQynM')
        first_card = result_list[0]
        a_tag = first_card.find_element(By.CLASS_NAME, 'item-root').find_element(By.TAG_NAME, 'a')
        match_result_url = a_tag.get_attribute('href')
        douban_id = get_douban_id(match_result_url)
        douban_row = {'imdb_id': imdb_id, 'douban_id': douban_id}
        print('douban_row', douban_row)
    except Exception as er:
        save_error_log(connection=connection, start=start, file_name=file_name, func_name=func, status=2,
                   imdb_id=imdb_id, msg=str(er))
        return 2
    try:
        # save to sql movie.douban
        insert_dict_into_sql(connection, 'movie_id_mapping', douban_row, ignore=True)
        mark_column(connection, 'imdb_movie_id', 'douban_01', 1, 'imdb_id', imdb_id)
        return 1

    except Exception as er:
        save_error_log(connection=connection, start=start, file_name=file_name, func_name=func, status=3,
                       imdb_id=imdb_id, msg=str(er))
        return 2


def get_imdb_id_list_douban_01_null():
    engine = create_conn_pool()
    connection = get_connection(engine)
    query = "SELECT imdb_id FROM movie.imdb_movie_id  WHERE type ='Movie' and douban_01 <=> NULL;"
    imdb_tuple_list = connection.execute(query).fetchall()
    imdb_list = [my_tuple[0]for my_tuple in imdb_tuple_list]
    connection.close()
    engine.dispose()
    return imdb_list


def multi_thread_douban_01_by_list(worker_num):
    file_name = os.path.basename(__file__)
    imdb_list = get_imdb_id_list_douban_01_null()
    # imdb_list = ['tt9909186', 'tt9900782']
    print('job num:', len(imdb_list))
    imdb_detail_crawler = Multi_thread_selenium_crawler(imdb_list, worker_num, douban_01_by_id, file_name)
    imdb_detail_crawler.create_worker()


multi_thread_douban_01_by_list(worker_num=2)
