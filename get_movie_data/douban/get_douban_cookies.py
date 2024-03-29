import os
import time
import inspect

from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from datetime import datetime, timezone, timedelta

# Local application imports
from local_package.web_crawler_tools.multi_thread import MultiThread
from local_package.db.mongodb import insert_mongo_doc, create_mongo_connection
from local_package.db.mysql import save_error_log


file_name = os.path.basename(__file__)


def get_current_taiwan_datetime():
    current_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    convert_time = current_time.astimezone(timezone(timedelta(hours=8)))
    return convert_time


def get_selenium_cookies(sql_conn):
    # create driver
    user_agent = UserAgent(use_cache_server=False).random
    options = Options()
    options.add_argument(f'user-agent={user_agent}')
    options.add_argument("start-maximized")
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    url = 'https://movie.douban.com/subject/35914264/'

    try:
        driver.get(url)
    except Exception as er:
        log_info = {
            'connection': sql_conn,
            'start': time.time(),
            'file_name': file_name,
            'func_name': inspect.stack()[0][3],
        }
        save_error_log(status=2, imdb_id='douban_cookies', msg=str(er), log_info=log_info)
        return 2

    cookies = driver.get_cookies()
    cookies_dict = {}
    for item in cookies:
        cookies_dict[item['name']] = item['value']
    driver.quit()
    return cookies_dict


def save_douban_cookies_mongo_doc(doc_no, sql_conn):
    mongo_db, mongo_conn = create_mongo_connection()
    my_cookies = get_selenium_cookies(sql_conn)
    date_ymd = get_current_taiwan_datetime().strftime('%Y-%m-%d')
    # merge data
    data = {
        "created_date": date_ymd,
        'cookies_list': my_cookies
    }
    insert_mongo_doc(db=mongo_db, collection_name='douban_cookies', doc=data)
    mongo_conn.close()
    return 1


def save_douban_cookies_into_mongo(target_docs_num, worker_num):
    mongo_db, mongo_conn = create_mongo_connection()
    # clear all content in mongodb collection: movie.douban_cookies
    douban_cookies_collection = mongo_db.douban_cookies
    douban_cookies_collection.delete_many({})

    # create douban cookies by requesting douban website by selenium,
    resource_list = ['sql_conn']
    id_list = [{'doc_no': i} for i in range(target_docs_num)]
    multi_thread_cookies_creator = MultiThread(id_list=id_list,
                                               worker_num=worker_num,
                                               func=save_douban_cookies_mongo_doc,
                                               file_name=file_name,
                                               table='',
                                               resource_list=resource_list
                                               )
    multi_thread_cookies_creator.create_worker()
    mongo_conn.close()
