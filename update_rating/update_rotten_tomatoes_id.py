from package.db.sql import insert_dict_list_into_db, MySQL, update_on_duplicate_key
import requests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from package.nlp import match_preprocessing
import time
import os
from datetime import date
from package.multi_thread import MultiThread

file_name = os.path.basename(__file__)


# input: 'top gun'
# output:  [{'year': '2022', 'movie_url': 'https://www.rottentomatoes.com/m/top_gun_maverick', 'movie_name': 'Top Gun: Maverick', 'cast':...
def find_movie_on_tomato(search_name):
    user_agent = UserAgent(use_cache_server=False).random
    headers = {
        'User-Agent': user_agent,
        'accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9',
    }

    # get html data
    search_api = 'https://www.rottentomatoes.com/search?search='
    search_str = search_name.replace(' ', '%20')
    url = search_api + search_str
    response = requests.get(url=url, headers=headers, proxies={'https': '182.54.239.250:8267'})
    soup = BeautifulSoup(response.text, "html.parser")
    print('status_code', response.status_code)
    if response.status_code == 200:
        try:
            search_result = soup.find('search-page-result', {'type': 'movie'})
            if search_result is None:
                return None

            search_result_list = search_result.findAll('search-page-media-row')
            if not search_result_list:
                return None

            # create result dict
            result_list = []
            for tag in search_result_list:
                year = tag['releaseyear']
                cast = tag['cast'].split(',')
                movie_url = tag.find('a', {'slot': 'thumbnail'})['href']
                movie_name = tag.find('a', {'slot': 'title'}).text.strip()
                result_list.append({
                    'year': year,
                    'movie_url': movie_url,
                    'movie_name': movie_name,
                    'cast': cast
                })
            return result_list
        except Exception as er:
            print(er)
    else:
        return None


# input: 'top gun', 2022, [...]
# output: filtered_dict list
def condition_filter(target_movie_name, target_year, result_list):
    filtered_list = []
    for candidate in result_list:
        candidate_name = candidate['movie_name']
        candidate_year = candidate['year']

        matching_ratio = match_preprocessing(target_movie_name, candidate_name)
        try:
            if abs(int(target_year) - int(candidate_year)) <= 2 and matching_ratio >= 0.5:
                filtered_list.append(candidate)
        except:
            # lost year value in html page
            pass

    return filtered_list


def get_imdb_target_info(sql_conn, imdb_id):
    sql = MySQL()
    table = "(SELECT imdb_id, primary_title, start_year FROM movie.movie_info  where imdb_id = '{}') as t1".format(imdb_id)
    columns = '*'
    condition = """inner join
    (SELECT movie.staff.imdb_movie, movie.staff.job_type, movie.celebrity.en_name
    FROM movie.staff
    INNER JOIN movie.celebrity
    ON movie.staff.imdb_per = movie.celebrity.imdb_per) as t2
    on t1.imdb_id = t2.imdb_movie;"""
    result_dict_list = sql.fetch_data(sql_conn, table, columns, condition, 'dict')

    if not result_dict_list:
        return None
    target_movie = result_dict_list[0]['primary_title']
    year = result_dict_list[0]['start_year']
    cast = {person['en_name']: 1 for person in result_dict_list if person['job_type'] == 'actor'}
    out = {
        'target_movie': target_movie,
        'year': year,
        'cast': cast
    }
    return out


def cast_matcher(target_cast, candidate_cast):
    count_cast_mathing = 0
    for actor in candidate_cast:
        if actor in target_cast:
            count_cast_mathing = count_cast_mathing + 1
    return count_cast_mathing


# status code: 4: the staff list of target info is empty
def imdb_mapping_tomato_id(imdb_id, sql_conn, start_year):
    # STEP 0: get imdb target movie info
    # {'target_movie': 'Top Gun: Maverick', 'year': '2022', 'cast': {'Jennifer Connelly': 1, 'Tom Cruise': 1, 'Val Kilmer': 1, 'Jon Hamm': 1, 'Charles Parnell': 1, 'Jay Ellis': 1, 'Glen Powell': 1, 'Bashir Salahuddin': 1, 'Miles Teller': 1, 'Kara Wang': 1, 'Raymond Lee': 1, 'Manny Jacinto': 1, 'Monica Barbaro': 1, 'Jake Picking': 1, 'Lewis Pullman': 1, 'Danny Ramirez': 1, 'Jack Schumacher': 1, 'Greg Tarzan Davis': 1}}
    target_info = get_imdb_target_info(sql_conn, imdb_id)
    # print('target_info', target_info)
    if target_info is None:
        my_dict = {
            'imdb_id': imdb_id,
            'result': 4,
        }
        insert_dict_list_into_db(sql_conn, 'tomato_mapping', [my_dict], ignore=True)

        # no rotten_tomatoes_id match
        if start_year < 2022:
            update_on_duplicate_key(sql_conn, 'movie_id_mapping', [{'imdb_id': imdb_id, 'rotten_tomatoes_id': 0}], multi_thread=True)

        return 1

    target_name = target_info['target_movie']

    # STEP 1: request rottentomatoes.com search api
    result_list = find_movie_on_tomato(target_name)

    if result_list is None:  # page display "Sorry, no results found for <target name>"
        my_dict = {
            'imdb_id': imdb_id,
            'imdb_name': target_info['target_movie'],
            'result': 0,
            'imdb_year': target_info['year'],
        }
        insert_dict_list_into_db(sql_conn, 'tomato_mapping', [my_dict], ignore=True)

        # no rotten_tomatoes_id match
        if start_year < 2021:
            update_on_duplicate_key(sql_conn, 'movie_id_mapping', [{'imdb_id': imdb_id, 'rotten_tomatoes_id': 0}], multi_thread=True)
        return 1

    # STEP 2
    candidate_list = condition_filter(target_info['target_movie'], target_info['year'], result_list)

    # STEP 3: cast matcher
    final_matching_list = []
    target_cast = target_info['cast']
    for candidate in candidate_list:
        count_cast_mathing = cast_matcher(target_cast, candidate['cast'])
        if count_cast_mathing > 1:
            final_matching_list.append(candidate)

    # choose one
    if len(final_matching_list) >= 1:
        matching = final_matching_list[0]
        tomato_id = matching['movie_url'].replace('https://www.rottentomatoes.com/m/', '')

        my_dict = {
            'imdb_id': imdb_id,
            'imdb_name': target_info['target_movie'],
            'result': 1,
            'tomato_id': tomato_id,
            'tomato_name': matching['movie_name'],
            'tomato_year': matching['year'],
            'imdb_year': target_info['year'],
            'candidate': len(final_matching_list)
        }
        # recheck movie url is not null on rotten tomato search result e.g. 'Comrade Drakulich'
        if tomato_id != '':
            update_on_duplicate_key(sql_conn, 'tomato_mapping', [my_dict], multi_thread=True)
            update_on_duplicate_key(sql_conn, 'movie_id_mapping', [{'imdb_id': imdb_id, 'rotten_tomatoes_id': tomato_id}], multi_thread=True)
            return 1

    # no rotten_tomatoes_id match
    my_dict = {
        'imdb_id': imdb_id,
        'imdb_name': target_info['target_movie'],
        'imdb_year': target_info['year'],
        'result': 0,
        'candidate': len(final_matching_list)
    }
    update_on_duplicate_key(sql_conn, 'tomato_mapping', [my_dict], multi_thread=True)

    # do not find this movie again
    if start_year < 2021:
        update_on_duplicate_key(sql_conn, 'movie_id_mapping', [{'imdb_id': imdb_id, 'rotten_tomatoes_id': 0}], multi_thread=True)
    return 1


def get_imdb_list(sql_conn, year):
    query = """SELECT * from
    (SELECT movie_info.imdb_id, movie_info.start_year, movie_id_mapping.douban_id, movie_id_mapping.rotten_tomatoes_id FROM movie.movie_info
    INNER JOIN movie.movie_id_mapping
    ON movie.movie_info.imdb_id = movie.movie_id_mapping.imdb_id) as t1
    WHERE start_year > {} and douban_id is not null AND douban_id !=0 AND rotten_tomatoes_id IS NULL""".format(year)
    imdb_tuple_list = sql_conn.execute(query).fetchall()
    imdb_list = [{'imdb_id': my_tuple[0], 'start_year': int(my_tuple[1])}for my_tuple in imdb_tuple_list]

    return imdb_list


def rotten_tomatoes_total_count(sql_conn):
    sql = MySQL()
    table = """(SELECT movie_info.imdb_id, movie_info.start_year, movie_id_mapping.douban_id, movie_id_mapping.rotten_tomatoes_id 
    FROM movie.movie_info
    INNER JOIN movie.movie_id_mapping
    ON movie.movie_info.imdb_id = movie.movie_id_mapping.imdb_id) as t1
    """""
    condition = 'WHERE start_year > 2017 AND douban_id IS NOT NULL AND douban_id != 0 AND rotten_tomatoes_id IS NOT NULL AND rotten_tomatoes_id !="0"'
    rotten_tomatoes_id_mapping_imdb_list = sql.fetch_data(conn=sql_conn, table=table, columns=['rotten_tomatoes_id'], struct='list', condition=condition)
    return len(rotten_tomatoes_id_mapping_imdb_list)


def multi_thread_rotten_tomatoes_id(worker_num, year):
    start = time.time()
    sql = MySQL()
    engine, sql_conn = sql.get_connection()

    # get imdb_list
    id_list = get_imdb_list(sql_conn, year)

    old_rotten_tomatoes_id_count = rotten_tomatoes_total_count(sql_conn)
    # mapping by multi-thread
    multi_thread_cookies_creator = MultiThread(id_list=id_list,
                                               worker_num=worker_num,
                                               func=imdb_mapping_tomato_id,
                                               file_name=file_name,
                                               table='tomato_mapping',
                                               resource_list=['sql_conn']
                                               )
    multi_thread_cookies_creator.create_worker()

    end = time.time()
    second_sql_conn = engine.connect()  # avoid mysql cache
    new_rotten_tomatoes_id_count = rotten_tomatoes_total_count(second_sql_conn)

    dashboard_log = {
        'log_time': date.fromtimestamp(start),
        'rotten_tomatoes_old_count': old_rotten_tomatoes_id_count,
        'rotten_tomatoes_id_count': new_rotten_tomatoes_id_count,
        'rotten_tomatoes_new_count': new_rotten_tomatoes_id_count - old_rotten_tomatoes_id_count,
        'tomato_id_time': round(end-start, 0)
    }

    update_on_duplicate_key(sql_conn, 'dashboard_movie_count', [dashboard_log], True)

    engine.dispose()


multi_thread_rotten_tomatoes_id(worker_num=20, year=2017)

