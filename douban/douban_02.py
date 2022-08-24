from package.db.mongo import create_mongo_connection
from bs4 import BeautifulSoup
from package.db.sql import save_error_log, create_conn_pool, get_connection, mark_column, save_processing_log
import os
from datetime import datetime
import requests
import inspect
from package.db.mongo import insert_mongo_doc
from package.db.s3 import put_photo_to_s3
from package.general import get_current_taiwan_datetime
from package.multi_thread import Beautifulsoup_multi_thread_crawler
from fake_useragent import UserAgent
import pymongo


file_name = os.path.basename(__file__)


def get_douban_id_dict_list():
    engine = create_conn_pool()
    connection = get_connection(engine)

    # get imdb_id list
    query = """select movie.movie_id_mapping.imdb_id, movie.movie_id_mapping.douban_id
    from (select imdb_id from movie.imdb_movie_id where movie.imdb_movie_id.douban_01 = 1 and movie.imdb_movie_id.douban_02 <=> NULL) as t1
    INNER JOIN movie.movie_id_mapping
    ON t1.imdb_id = movie.movie_id_mapping.imdb_id;"""
    dict_list = connection.execute(query).mappings().fetchall()
    # [{'imdb_id': 'tt0099396', 'douban_id': '5114650'}...]
    connection.close()
    engine.dispose()
    return dict_list


# input: https://img9.doubanio.com/f/movie/30c6263b6db26d055cbbe73fe653e29014142ea3/pics/movie/movie_default_large.png
# output: movie_default_large.png
def get_img_name(url):
    pos = url.rfind('/') + 1
    return url[pos:]


# status code: 1: data & img ok, 2: data & img error, 3: data ok & img error, 4: page not exist
def beautifulsoup_douban_detail_page_into_mongo(sql_conn, mongo_conn, id_dict, cookies_doc, proxy_list):
    print('id_dict', id_dict)
    proxy = proxy_list.pop()
    imdb_id = id_dict['imdb_id']
    douban_id = id_dict['douban_id']
    func_name = inspect.stack()[0][3]
    start = get_current_taiwan_datetime()

    user_agent = UserAgent(use_cache_server=False).random
    headers = {
        'User-Agent': user_agent,
        'accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9',
    }

    # get html data
    url = 'https://movie.douban.com/subject/' + douban_id
    try:
        my_cookies = cookies_doc['cookies_list']
        response = requests.get(url=url, headers=headers, proxies={'https': proxy}, cookies=my_cookies)
        print('status code', response.status_code)
        print('response.text', len(response.text))
        soup = BeautifulSoup(response.text, "html.parser")

        # anti-spider: return the empty content or 403
        if response.status_code == 403 or soup is None or soup == '':
            proxy = proxy_list.pop()
            response = requests.get(url, headers=headers, proxies={'https': proxy})
            soup = BeautifulSoup(response.text, "html.parser")

        # anti-spider: lost part of the content
        if len(response.text) < 3000:
            save_processing_log(sql_conn, start, file_name, func_name, 5, imdb_id)
            proxy_list.append(proxy)
            return 2

        if soup.title == '页面不存在':
            mark_column(sql_conn, 'imdb_movie_id', 'douban_img', 4, 'imdb_id', imdb_id)
            proxy_list.append(proxy)
            return 1

    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func_name, status=2,
                       imdb_id=imdb_id, msg=str(er))
        proxy_list.append(proxy)
        return 2

    # save data to mongo
    try:
        date_ymd = datetime.now().strftime('%Y-%m-%d')
        doc = {"created_date": date_ymd, 'imdb_id': imdb_id, 'douban_id': douban_id, 'page': response.text}
        # save data to mongo
        mongo_db, mongo_conn = create_mongo_connection()
        insert_mongo_doc(mongo_db, 'douban_02_detail_page', doc)
        mark_column(sql_conn, 'imdb_movie_id', 'douban_02', 1, 'imdb_id', imdb_id)
    except pymongo.errors.DuplicateKeyError:
        pass
    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func_name, status=3,
                       imdb_id=imdb_id, msg=str(er))
        proxy_list.append(proxy)
        return 3

    # get movie image
    try:
        image_div = soup.find('div', {'id': 'mainpic'})
        img_url = image_div.find('img')['src']
        original_img_name = get_img_name(img_url)
        # save img to s3
        try:
            if original_img_name != 'movie_default_large.png':  # img not empty
                img_response = requests.get(img_url, headers=headers, proxies={'https': proxy})
                data = img_response.content
                photo_name = douban_id + '_main.webp'
                put_photo_to_s3(photo_name=photo_name, photo_stream=data)
                mark_column(sql_conn, 'imdb_movie_id', 'douban_img', 1, 'imdb_id', imdb_id)
            else:
                mark_column(sql_conn, 'imdb_movie_id', 'douban_img', 0, 'imdb_id', imdb_id)

            save_processing_log(connection=sql_conn, start=start, file_name=file_name, func_name=func_name, status=1,
                                imdb_id=imdb_id)
            proxy_list.append(proxy)
            return 1

        except Exception as er:
            save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func_name, status=3,
                           imdb_id=imdb_id, msg=str(er))
            mark_column(sql_conn, 'imdb_movie_id', 'douban_img', 3, 'imdb_id', imdb_id)
            proxy_list.append(proxy)
            return 3

    except Exception as er:
        # print(douban_id, 'response.status_code', response.status_code, 'error soup', soup)
        save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func_name, status=0,
                       imdb_id=imdb_id, msg=str(er))
        mark_column(sql_conn, 'imdb_movie_id', 'douban_img', 0, 'imdb_id', imdb_id)
        proxy_list.append(proxy)
        return 3


def multi_thread_douban_02_by_list(worker_num, file_name):
    # [{'imdb_id': 'tt0099396', 'douban_id': '5114650'}...]
    douban_dict_list = get_douban_id_dict_list()
    print('job num:', len(douban_dict_list))
    imdb_detail_crawler = Beautifulsoup_multi_thread_crawler(id_list=douban_dict_list, worker_num=worker_num, func=beautifulsoup_douban_detail_page_into_mongo, file_name=file_name)
    imdb_detail_crawler.create_worker()


# multi_thread_douban_02_by_list(worker_num=5, file_name=file_name)
