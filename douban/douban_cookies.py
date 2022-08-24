import time
import os
from package.multi_thread import Multi_thread
from package.crawler import get_selenium_cookies
from package.general import get_current_taiwan_datetime
from package.db.mongo import insert_mongo_doc, create_mongo_connection

def save_douban_cookies_mongo_doc(sql_conn, url_id):
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

file_name = os.path.basename(__file__)

def save_douban_cookies_into_mongo(num, worker_num):
    start = time.time()
    id_list = [i for i in range(num)]
    multi_thread_cookies_creator = Multi_thread(id_list, worker_num, save_douban_cookies_mongo_doc, file_name)
    multi_thread_cookies_creator.create_worker()
    end = time.time()
    print('spent', end - start)

# save_douban_cookies_into_mongo(2000, 10)
