import os
import requests
import inspect
import pymongo.errors
import time

from datetime import datetime
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

# Local application imports
from local_package.db.mongodb import insert_mongo_doc
from local_package.db.s3 import put_photo_to_s3
from local_package.db.mysql import save_error_log, MySQL, update_on_duplicate_key
from local_package.web_crawler_tools.multi_thread import DoubaonCookiesCrawler
from get_movie_data.douban.get_douban_cookies import save_douban_cookies_into_mongo

file_name = os.path.basename(__file__)


# input: https://img9.doubanio.com/f/movie/30c6263b6db26d055cbbe73fe653e29014142ea3/pics/movie/movie_default_large.png
# output: movie_default_large.png
def get_img_name(url):
    pos = url.rfind('/') + 1
    return url[pos:]


# status code: 1: data & img ok, 2: data & img error, 4: page not exist
def scrapying_douban_detail_page_and_save_img(imdb_id, douban_id, sql_conn, mongo_conn, douban_cookies_doc, proxy):

    # mysql table: 'movie.processing_finished_log'
    log_info = {
        'connection': sql_conn,
        'start': time.time(),
        'file_name': file_name,
        'func_name': inspect.stack()[0][3],
    }

    user_agent = UserAgent().random
    headers = {
        'User-Agent': user_agent,
        'accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate',  # don not use br (Brotli compression)
        'accept-language': 'zh-CN,zh;q=0.9',
    }

    # get html data
    url = 'https://movie.douban.com/subject/' + douban_id
    try:
        my_cookies = douban_cookies_doc['cookies_list']
        response = requests.get(url=url, headers=headers, proxies={'https': proxy}, cookies=my_cookies)
        soup = BeautifulSoup(response.text, "html.parser")

        # anti-spider: return the empty content or 403
        try:
            title_element = soup.find('title')
            if response.status_code == 403 or soup is None or soup == '':
                return 4

            if title_element is not None:
                cn_title = title_element.text.replace(' (豆瓣)', '').strip()
                if cn_title == '豆瓣 - 登录跳转页':
                    return 4
            else:
                save_error_log(status=2, imdb_id=imdb_id, msg='title_element is None', log_info=log_info, exc=False)
                return 2

        except Exception as er:
            save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
            return 2

        # anti-spider: lost part of the content
        if len(response.text) < 3000:
            return 4

        if soup.title == '页面不存在':
            return 1
    except requests.exceptions.ProxyError as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        return 2
    except Exception as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        return 2

    # find json data of movie info in html
    str_data = ''  # default
    try:
        find_json = soup.find('script', {'type': 'application/ld+json'})
        if find_json is not None:
            str_data = find_json.text
    except Exception as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        return 2

    # find element id='info'
    str_find_id_div = ''  # default value
    try:
        find_id_div = soup.find('div', {'id': 'info'})
        if find_id_div is not None:
            str_find_id_div = find_id_div.text.replace('  ', ' ')
    except Exception as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        return 1
    # summary
    str_summary = ''  # default value
    try:
        find_summary = soup.find('span', {'property': 'v:summary'})
        if find_summary is not None:
            str_summary = find_summary.text.replace('\n', '').strip()

    except Exception as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        return 1

    # save data to mongo
    try:
        date_ymd = datetime.now().strftime('%Y-%m-%d')

        doc = {
            "created_date": date_ymd,
            'imdb_id': imdb_id,
            'douban_id': douban_id,
            'cn_movie_title': cn_title,
            'html_json_data': str_data,
            'html_info': str_find_id_div,
            'html_summary': str_summary
        }
        # save data to mongo
        insert_mongo_doc(mongo_conn, 'douban_02_detail_page', doc)

    except pymongo.errors.DuplicateKeyError as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
    except Exception as er:
        save_error_log(status=3, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        return 2

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
                douban_row = {
                    'imdb_id': imdb_id,
                    'img': douban_id + '_main.webp'
                }
                update_on_duplicate_key(sql_conn, 'movie_info', [douban_row], multi_thread=True)
                return douban_row

            else:
                douban_row = {'imdb_id': imdb_id, 'img': 0}
                update_on_duplicate_key(sql_conn, 'movie_info', [douban_row], multi_thread=True)
                return douban_row

        except Exception as er:
            save_error_log(status=3, imdb_id=imdb_id, msg=str(er), log_info=log_info)
            return 2

    except Exception as er:
        save_error_log(status=0, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        douban_row = {'imdb_id': imdb_id, 'img': 0}
        update_on_duplicate_key(sql_conn, 'movie_info', [{'imdb_id': imdb_id, 'img': 0}], multi_thread=True)
        return douban_row


def get_douban_id_dict_list():
    sql = MySQL()
    engine, connection = sql.get_connection()

    # get imdb_id list
    query = """SELECT t1.douban_id, t2.imdb_id FROM
    (SELECT imdb_id, douban_id FROM movie.movie_id_mapping where douban_id is not null and douban_id !=0) as t1
    INNER JOIN (SELECT imdb_id FROM movie.movie_info where img IS null and start_year > 2017) AS t2
    ON t1.imdb_id = t2.imdb_id;"""
    dict_list = connection.execute(query).mappings().fetchall()
    # [{'imdb_id': 'tt0099396', 'douban_id': '5114650'}...]
    connection.close()
    engine.dispose()
    return dict_list


def multi_thread_scrape_douban_detail_page_and_save_img(crawler_worker_num, cookies_worker_num, target_docs_num):
    # step 1: scrapying douban movie detail pages
    function_resource_args_list = ['sql_conn', 'mongo_conn']
    imdb_list = get_douban_id_dict_list()

    # step 2 : create douban cookies
    if len(imdb_list) > 0:
        save_douban_cookies_into_mongo(target_docs_num=target_docs_num, worker_num=cookies_worker_num)

    multi_thread_douban_crawler = DoubaonCookiesCrawler(
        id_list=imdb_list,
        worker_num=crawler_worker_num,
        func=scrapying_douban_detail_page_and_save_img,
        file_name=file_name,
        table='movie_info',
        resource_list=function_resource_args_list)
    multi_thread_douban_crawler.create_worker()
