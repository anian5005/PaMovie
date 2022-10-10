import os
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from datetime import datetime, timezone, timedelta
from package.multi_thread import MultiThread
from package.db.mongo import insert_mongo_doc, create_mongo_connection


file_name = os.path.basename(__file__)


def get_current_taiwan_datetime():
    current_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    convert_time = current_time.astimezone(timezone(timedelta(hours=8)))
    return convert_time


def get_selenium_cookies():
    # create driver
    user_agent = UserAgent(use_cache_server=False).random
    options = Options()
    options.add_argument(f'user-agent={user_agent}')
    options.add_argument("start-maximized")
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    url = 'https://movie.douban.com/subject/10001432/'
    driver.get(url)
    cookies = driver.get_cookies()
    cookies_dict = {}
    for item in cookies:
        cookies_dict[item['name']] = item['value']
    driver.quit()
    return cookies_dict


def save_douban_cookies_mongo_doc(doc_no, sql_conn):
    mongo_db, mongo_conn = create_mongo_connection()
    my_cookies = get_selenium_cookies()
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



