import requests
import gzip
import inspect
import os
import time
from time import sleep
import json
from datetime import date
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from package.multi_thread import DoubaonCookiesCrawler, MultiThread
from package.db.sql import save_error_log, MySQL, update_on_duplicate_key, recheck_if_error
from package.omdb_api import multi_thread_update_tomato_rating_by_omdb_api
from douban.get_douban_cookies import save_douban_cookies_into_mongo


file_name = os.path.basename(__file__)
# SAVE_PATH = '/home/ec2-user/airflow_temp/update_rating/'
SAVE_PATH = "\\Users\\enisi\\PycharmProjects\\movie\\update_rating\\"


# id_dict {'imdb_id': tt55555555, 'douban_id':123}
def update_douban_rating(imdb_id, douban_id, sql_conn, douban_cookies_doc, proxy):
    log_date = date.fromtimestamp(time.time())

    # log_info
    log_info = {
        'connection': sql_conn,
        'start': time.time(),
        'file_name': file_name,
        'func_name': inspect.stack()[0][3],
    }

    user_agent = UserAgent(use_cache_server=False).random
    headers = {
        'User-Agent': user_agent,
        'accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate',
        'accept-language': 'zh-CN,zh;q=0.9',
    }
    url = 'https://movie.douban.com/subject/' + douban_id
    try:
        my_cookies = douban_cookies_doc['cookies_list']

        response = requests.get(url=url, headers=headers, proxies={'https': proxy}, cookies=my_cookies)
        print('status code', response.status_code)
        soup = BeautifulSoup(response.text, "html.parser")

        # anti-spider: return the empty content or 403
        cn_title = soup.find('title').text.replace(' (豆瓣)', '').strip()
        if response.status_code == 403 or soup is None or soup == '' or cn_title == '豆瓣 - 登录跳转页':
            return 4

        # anti-spider: lost part of the content
        if len(response.text) < 3000:
            return 4

        if soup.title == '页面不存在':
            save_error_log(status=2, imdb_id=imdb_id, msg='Page Or ID Not Found', log_info=log_info)
            return 1
    except requests.exceptions.ProxyError:
        return 4

    except requests.exceptions.TooManyRedirects:
        return 4

    except Exception as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        return 2

    # find json data of movie info in html
    try:
        find_json = soup.find('script', {'type': 'application/ld+json'})
        if find_json is not None:
            str_data = find_json.text
            json_data = json.loads(str_data, strict=False)
            if "aggregateRating" in json_data:
                rating_value = json_data['aggregateRating']["ratingValue"]
                rating_count = json_data['aggregateRating']['ratingCount']
                if rating_value != '':
                    data_dict = {
                        'imdb_id': imdb_id,
                        'douban_rating': float(rating_value),
                        'douban_votes': int(rating_count),
                        'douban_updated': log_date
                    }
                    # print('data_dict', data_dict)
                    update_on_duplicate_key(sql_conn, 'movie_rating', [data_dict], multi_thread=True)
                    return 1

                elif rating_value == '':
                    # data_dict {'imdb_id': 'tt0013274', 'douban_rating': '', 'douban_votes': ''}
                    data_dict = {
                        'imdb_id': imdb_id,
                        'douban_rating': None,
                        'douban_votes': 0,
                        'douban_updated': log_date
                    }
                    update_on_duplicate_key(sql_conn, 'movie_rating', [data_dict], multi_thread=True)
                    return 1

        return 1

    except Exception as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        return 2


def multi_thread_update_douban_rating(worker_num, id_list, target_docs_num):
    save_douban_cookies_into_mongo(target_docs_num=target_docs_num, worker_num=1)
    function_resource_args_list = ['sql_conn', 'douban_cookies', 'proxy']
    multi_thread_douban_crawler = DoubaonCookiesCrawler(
        id_list=id_list,
        worker_num=worker_num,
        func=update_douban_rating,
        file_name=file_name,
        table='movie_rating',
        resource_list=function_resource_args_list)
    multi_thread_douban_crawler.create_worker()


def update_imdb_rating(imdb_list):
    log_date = date.fromtimestamp(time.time())
    sql = MySQL()
    engine, sql_conn = sql.get_connection()

    imdb_dataset_hash = {}
    data_list = []
    no_imdb_rating_list = []

    # download imdb dataset
    url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
    filename = url.split("/")[-1]
    with open(SAVE_PATH+filename, "wb") as f:
        r = requests.get(url)
        f.write(r.content)
    # create imdb_dataset_hash table
    with gzip.open(SAVE_PATH+'title.ratings.tsv.gz', "rb") as unzip_file:
        for idx, line in enumerate(unzip_file):
            content_list = line.decode('utf-8').rstrip('\n').split('\t')
            # content_list ['tt0012830', '6.1', '22']
            imdb_id = content_list[0]

            movie_rating_data = {
                'imdb_id': imdb_id,
                'imdb_rating': content_list[1],
                'imdb_votes': content_list[2],
                'imdb_updated': log_date
            }
            imdb_dataset_hash.update({imdb_id: movie_rating_data})

    # find imdb_id
    for find_imdb_id in imdb_list:
        if find_imdb_id in imdb_dataset_hash:
            data = imdb_dataset_hash[find_imdb_id]
            data_list.append(data)
        else:
            no_imdb_rating = {'imdb_id': imdb_id, 'imdb_updated': log_date}
            no_imdb_rating_list.append(no_imdb_rating)

    update_on_duplicate_key(sql_conn, 'movie_rating', data_list, multi_thread=True)
    sql_conn.close()
    engine.dispose()
    os.unlink(SAVE_PATH + 'title.ratings.tsv.gz')
    return data_list, no_imdb_rating_list


def get_imdb_list(start_year):
    sql = MySQL()
    engine, connection = sql.get_connection()
    query = """SELECT imdb_id, douban_id, rotten_tomatoes_id from
    (SELECT movie_info.start_year, movie_id_mapping.* FROM movie.movie_info
    INNER JOIN movie.movie_id_mapping
    ON movie.movie_info.imdb_id = movie.movie_id_mapping.imdb_id) as t1
    WHERE start_year >= {} and douban_id is not null;""".format(start_year)
    imdb_tuple_list = connection.execute(query).fetchall()
    imdb_list = [my_tuple[0] for my_tuple in imdb_tuple_list]

    # imdb mapping rotten_tomatoes_id list
    tomato_query = """SELECT imdb_id, rotten_tomatoes_id from
    (SELECT movie_info.start_year, movie_id_mapping.* FROM movie.movie_info
    INNER JOIN movie.movie_id_mapping
    ON movie.movie_info.imdb_id = movie.movie_id_mapping.imdb_id) as t1
    WHERE start_year >= {} and douban_id !=0 and rotten_tomatoes_id is not null and rotten_tomatoes_id !='0';""".format(start_year)
    imdb_tuple_list = connection.execute(tomato_query).fetchall()
    tomato_mapping_imdb_dict_list = []
    for my_tuple in imdb_tuple_list:
        my_dict = {'imdb_id': my_tuple[0], 'rotten_tomatoes_id': my_tuple[1]}
        tomato_mapping_imdb_dict_list.append(my_dict)

    # douban list
    douban_query = """SELECT t2.imdb_id, t2.douban_id from 
    (SELECT t1.start_year, imdb_id, douban_id, rotten_tomatoes_id from
    (SELECT movie_info.start_year, movie_id_mapping.* FROM movie.movie_info
    INNER JOIN movie.movie_id_mapping
    ON movie.movie_info.imdb_id = movie.movie_id_mapping.imdb_id) as t1
    WHERE start_year =2022  and douban_id is not null and douban_id !=0) as t2
    left join movie.movie_rating
    on movie.movie_rating.imdb_id = t2.imdb_id"""
    douban_tuple_list = connection.execute(douban_query).fetchall()
    douban_dict_list = [{'imdb_id': my_tuple[0], 'douban_id': my_tuple[1]} for my_tuple in douban_tuple_list]

    connection.close()
    engine.dispose()
    print('imdb_list', len(imdb_list))
    print('tomato_mapping_imdb_dict_list', len(tomato_mapping_imdb_dict_list))
    return imdb_list, douban_dict_list, tomato_mapping_imdb_dict_list


# id_dict {'imdb_id': tt55555555, 'tomato_id': 'top_gun_maverick'}
def update_tomato_rating(imdb_id, rotten_tomatoes_id, sql_conn):
    log_info = {
        'connection': sql_conn,
        'start': time.time(),
        'file_name': file_name,
        'func_name': inspect.stack()[0][3],
    }
    user_agent = UserAgent(use_cache_server=False).random
    headers = {
        'User-Agent': user_agent,
        'accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9',
    }

    # get html data
    url = 'https://www.rottentomatoes.com/m/' + rotten_tomatoes_id
    sleep(15)
    try:
        response = requests.get(url=url, headers=headers, proxies={'https': '182.54.239.250:8267'})
        soup = BeautifulSoup(response.text, "html.parser")

        if response.status_code == 200:
            # defalt value if website not display this value
            rating_value = None
            rating_count = 0
            find_json = soup.find('script', {'type': 'application/ld+json'})
            if find_json is not None:
                str_data = find_json.text
                json_data = json.loads(str_data, strict=False)

                if 'aggregateRating' in json_data:
                    if 'ratingValue' in json_data['aggregateRating']:
                        rating_value = json_data['aggregateRating']['ratingValue']

                    if 'ratingCount' in json_data['aggregateRating']:
                        rating_count = json_data['aggregateRating']['ratingCount']

            # find_json is not None & find_json is  None, to do..
            tomato_dict = {
                'imdb_id': imdb_id,
                'tomatoes_rating': rating_value,
                'tomato_votes': rating_count,
                'tomatoes_source': 'crawler',
                'tomatoes_updated': date.fromtimestamp(time.time())
            }
            update_on_duplicate_key(sql_conn, 'movie_rating', [tomato_dict], multi_thread=True)

        else:
            msg = 'response.status_code {}'.format(response.status_code)
            save_error_log(status=2, imdb_id=imdb_id, msg=msg, log_info=log_info, exc=False)
        return 1
    except Exception as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info, exc=False)
        return 1


def multi_thread_update_tomato_rating(worker_num, id_list):
    multi_thread_cookies_creator = MultiThread(id_list=id_list,
                                               worker_num=worker_num,
                                               func=update_tomato_rating,
                                               file_name=file_name,
                                               table='movie_rating',
                                               resource_list=['sql_conn']
                                               )
    multi_thread_cookies_creator.create_worker()


def rating_updated_total_count(sql_conn, log_date):
    sql = MySQL()
    # imdb updated rating count
    imdb_condition = 'where DATE(imdb_updated) = "{}"'.format(log_date)
    imdb_updated_rating_count = sql.fetch_data(sql_conn, 'movie_rating', ['imdb_id'], imdb_condition, 'list')

    # tomatoes updated rating count
    tomatoes_condition = 'WHERE DATE(tomatoes_updated) = "{}" AND tomatoes_source="crawler" '.format(log_date)
    tomatoes_updated_rating_count = sql.fetch_data(sql_conn, 'movie_rating', ['imdb_id'], tomatoes_condition, 'list')

    # douban updated rating count
    query = """SELECT movie_info.start_year, movie_rating.douban_updated FROM movie.movie_rating
    INNER JOIN movie.movie_info
    ON movie_info.imdb_id = movie_rating.imdb_id
    WHERE start_year =2022 and DATE(douban_updated) ='{}'""".format(log_date)
    douban_result_list = sql_conn.execute(query).fetchall()
    douban_updated_rating_count = len(douban_result_list)

    return len(imdb_updated_rating_count), len(tomatoes_updated_rating_count), douban_updated_rating_count, imdb_updated_rating_count


def multi_thread_update_rating():
    sql = MySQL()
    engine, sql_conn = sql.get_connection()
    start = time.time()
    log_timestamp_date = date.fromtimestamp(start)

    # get movie id list which needs to update
    imdb_list, douban_mapping_imdb_list, tomato_mapping_imdb_list = get_imdb_list(2018)

    print('job num:', len(imdb_list))

    # update imdb rating
    inserted_imdb_data_list, imdb_no_rating_list = update_imdb_rating(imdb_list)
    imdb_end = time.time()

    # update tomato rating
    multi_thread_update_tomato_rating(worker_num=20, id_list=tomato_mapping_imdb_list)
    multi_thread_update_tomato_rating_by_omdb_api(2017)
    tomato_end = time.time()

    # update douban rating
    multi_thread_update_douban_rating(worker_num=25, id_list=douban_mapping_imdb_list, target_docs_num=40)
    douban_end = time.time()

    # douban no rating
    douban_no_rating_condition = "where douban_updated = '{}' and douban_votes=0".format(log_timestamp_date)
    douban_no_rating_list = sql.fetch_data(conn=sql_conn, table='movie_rating', columns=['imdb_id'], struct='list', condition=douban_no_rating_condition)

    # tomato no rating
    tomato_no_rating_condition = "where tomatoes_updated = '{}' and tomato_votes=0 and tomatoes_source='crawler' ".format(log_timestamp_date)
    tomato_no_rating_list = sql.fetch_data(conn=sql_conn, table='movie_rating', columns=['imdb_id'], struct='list', condition=tomato_no_rating_condition)

    tomato_omdb_condition = "where tomatoes_updated = '{}' and tomatoes_source='omdb' ".format(log_timestamp_date)
    tomato_omdb_list = sql.fetch_data(conn=sql_conn, table='movie_rating', columns=['imdb_id'], struct='list', condition=tomato_omdb_condition)

    # recheck data pipeline result & error count
    imdb_updated_rating_count, tomatoes_updated_rating_count, douban_updated_rating_count, imdb_updated_list = rating_updated_total_count(sql_conn, log_timestamp_date)

    # imdb error count
    database_updated_imdb_hash = {i: 1 for i in imdb_updated_list}
    insert_imdb_hash = {i['imdb_id']: 1 for i in inserted_imdb_data_list}
    with open(SAVE_PATH + 'imdb_rating_updated_error.txt', 'a') as f:
        for j in database_updated_imdb_hash:
            if j not in insert_imdb_hash:
                f.write(log_timestamp_date.strftime("%Y-%m-%d") + '\t' + j + '\n')

    # douban error count
    douban_error_count = recheck_if_error(sql_conn, log_timestamp_date, func_name='update_douban_rating')

    # tomato error count
    tomato_crawler_error_count = recheck_if_error(sql_conn, log_timestamp_date, func_name='update_tomato_rating')

    if len(imdb_list) == len(inserted_imdb_data_list) + len(imdb_no_rating_list):
        result_of_imdb_updating = 0
    else:
        result_of_imdb_updating = 1

    dashboard_log = {
        'log_time': log_timestamp_date,
        'imdb_to_update': len(imdb_list),
        'imdb_rating_time': round((imdb_end - start), 0),
        'imdb_error_count': result_of_imdb_updating,
        'imdb_updated_count': len(inserted_imdb_data_list),
        'imdb_no_rating_count': len(imdb_no_rating_list),
        'rotten_tomatoes_rating_time': round((tomato_end - imdb_end), 0),
        'tomato_to_update': len(tomato_mapping_imdb_list),
        'rotten_tomatoes_error_count': tomato_crawler_error_count,
        'rotten_tomatoes_updated_count':  tomatoes_updated_rating_count,
        'tomato_omdb_count': len(tomato_omdb_list),
        'tomato_no_rating_count': len(tomato_no_rating_list),
        'douban_to_update': len(douban_mapping_imdb_list),
        'douban_rating_time': round((douban_end - tomato_end), 0),
        'douban_error_count': douban_error_count,
        'douban_updated_count': douban_updated_rating_count,
        'douban_no_rating_count': len(douban_no_rating_list)
    }
    update_on_duplicate_key(sql_conn, 'dashboard_rating_count', [dashboard_log], True)

    sql_conn.close()
    engine.dispose()
