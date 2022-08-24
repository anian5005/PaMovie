from bs4 import BeautifulSoup
from package.db.mongo import get_mongo_doc_by_date, get_mongo_doc_by_id, insert_mongo_doc
from package.general import get_current_taiwan_datetime
from opencc import OpenCC
import json
import re
from urllib.parse import unquote
from datetime import datetime
from package.db.sql import create_conn_pool, get_connection, insert_dict_list_into_sql, update_multi_col_by_id, sql_select_column, mark_column, save_error_log, update_one_col_by_id
from package.db.mongo import create_mongo_connection
import os
import inspect
from package.multi_thread import Mongo_multi_thread
import html

file_name = os.path.basename(__file__)


# input: '/celebrity/1313149/'
def filter_douban_id(url_str):
    douban_id = url_str.split('celebrity/')[1].rstrip('/')
    return douban_id


def split_name(title_str):
    en_name = title_str.split(' ', 1)[1]
    cn_name = title_str.split(' ')[0]
    return cn_name, en_name


# input: ['捍卫战士：独行侠(台)', '壮志凌云独行侠', '壮志凌云：独行者', '壮志凌云2：马弗里克', 'TopGun2', 'TopGunMaverick']
# output: {'tw': '捍卫战士：独行侠', 'more': ['壮志凌云独行侠', '壮志凌云：独行者', '壮志凌云2：马弗里克', 'TopGun2', 'TopGunMaverick']}
def get_other_name(imdb_id, name_list, container):
    # container = movie_names
    sql_list = []
    mapping = {'(台)': 'tw', '(港)': 'hk', '(臺)': 'tw'}

    # input: '捍卫战士：独行侠(台)', output: T or F, {'tw': '捍卫战士：独行侠'}
    def find_country(my_str):
        for word in mapping:
            if word in my_str:
                movie_name = my_str.replace(word, '')
                lang_type = mapping[word]
                container.update({lang_type: html.unescape(movie_name)})
                sql_list.append({'imdb_id': imdb_id, 'lang_type': lang_type, 'movie_name': movie_name})
                return True
        return False
    FIND_UP = 0
    other_name = []
    for name in name_list:
        # find yet
        if FIND_UP < 2 and find_country(name):
            FIND_UP = FIND_UP + 1
            # str contain country word e.g. (台)
            pass
        else:
            other_name.append(name)
            sql_list.append({'imdb_id': imdb_id, 'lang_type': 0, 'movie_name': name})
            # add_item_into_dict(container, 'more', str, struct='list')
    container.update({'others': other_name})
    return sql_list

# movie_dict =
#     {'name':
#          {'cn': '壯志凌雲2：獨行俠 Top Gun: Maverick',
#           'tw': '捍卫战士：独行侠',
#           'more': ['壮志凌云独行侠', '壮志凌云：麦德林', '壮志凌云：独行者', '壮志凌云：独行侠', '壮志凌云2：马弗里克', 'TopGun2', 'TopGunMaverick']
#           },
#      'datePublished': '2022-05-18',
#      'genre': ['動作'],
#      'language': '英语',
#      'runtime': '131'}


def clean_douban_02_detail_page_and_insert(sql_conn, mongo_db, imdb_id):
    start = get_current_taiwan_datetime()
    func = inspect.stack()[0][3]
    try:
        docs = get_mongo_doc_by_id(mongo_db, 'douban_02_detail_page', 'imdb_id', imdb_id)
        document = docs[0]
    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func, status=2,
                       imdb_id=imdb_id, msg=str(er))
        return 2

    # Convert setting from zh-CN to zh-TW
    cc = OpenCC('s2tw')
    movie = {}
    insert_movie_info = {}  # col: filming_loc, lang
    movie_names = {}
    date_ymd = datetime.now().strftime('%Y-%m-%d')
    imdb_id = document['imdb_id']
    douban_id = document['douban_id']
    movie.update({"created_date": date_ymd, 'imdb_id': imdb_id, 'douban_id': douban_id})
    print('imdb_id', imdb_id, 'douban_id', douban_id)

    # cn -> zh-tw char
    page = cc.convert(document['page'])
    soup = BeautifulSoup(page, 'lxml')
    try:
        find_json = soup.find('script', {'type': 'application/ld+json'})
    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func, status=2,
                       imdb_id=imdb_id, msg=str(er))
        return 2

    if find_json is None:
        mark_column(connection=sql_conn, table_name='imdb_movie_id', column_name='douban_03', status=0,
                    where_id='imdb_id', id_value=imdb_id)
        return
    else:
        str_data = find_json.text
        json_data = json.loads(str_data, strict=False)

    # get clean_celebrity_list
    clean_celebrity_list = []
    p_types = ['director', 'author', 'actor']
    p_safe_type = [i for i in p_types if i in json_data.keys()]
    for key in p_safe_type:
        person_list = json_data[key]
        if person_list:
            for person in person_list:
                url = person['url']
                zh_name, en_name = split_name(person['name'])
                p_id = filter_douban_id(url)
                p_dict = {'type': key, 'id': p_id, 'zh_name': zh_name, 'en_name': en_name}
                clean_celebrity_list.append(p_dict)
    if clean_celebrity_list:
        movie.update({'celebrity': clean_celebrity_list})

    # get basic info
    types = ['name', 'datePublished', 'genre']
    # check all info exist in doc
    safe_type = [i for i in types if i in json_data.keys()]
    for key in safe_type:
        info = json_data[key]
        if info != '':
            if key == 'name':
                movie_names.update({'cn': html.unescape(info)})
                movie.update({'name': movie_names})

            if key == 'genre':
                # info type is list
                # "genre": ["\u52a8\u4f5c"] -> '动作'
                clean_genre_list = [cc.convert(unquote(genre)) for genre in info]
                movie.update({key: clean_genre_list})
            if key == 'datePublished':
                movie.update({'published': info})

    # get find id='info'
    find_id_div = soup.find('div', {'id': 'info'})
    other_names_row_list = []
    if find_id_div is None:
        pass
    else:
        movie_info_list = find_id_div.text.replace('  ', ' ').split('\n')

        for i in movie_info_list:
            if ':' in i:
                key = i.split(':')[0]
                value = i.split(':')[1]
                if key == '又名':
                    other_name_list = value.split('/')
                    other_names_row_list = get_other_name(imdb_id, other_name_list, movie_names)
                    movie.update({'name': movie_names})
                elif key == '製片國家/地區':
                    movie.update({'filming_loc': value})
                    insert_movie_info.update({'filming_loc': value})
                elif key == '語言':
                    movie.update({'lang': value})
                    insert_movie_info.update({'lang': value})

        # runtime
        # <span content="131" property="v:runtime">131分钟</span><br/>
        try:
            find_runtime = find_id_div.find('span', {'property': 'v:runtime'})
            if find_runtime is None:
                pass
            else:
                runtime = find_runtime.text  # # 131分钟
                runtime = runtime.replace('分鐘', '')  # 131
                movie.update({'runtime': runtime})
        except Exception as er:
            save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func, status=2,
                           imdb_id=imdb_id, msg=str(er))

    # summary
    summary_dict = {}
    try:
        find_summary = soup.find('span', {'property': 'v:summary'})
        if find_summary is None:
            pass
        else:
            summary = find_summary.text.replace('\n', '').strip().replace('  ', ' ')
            zh__chart_detector = re.findall('[\u4e00-\u9fa5]', summary)
            if len(zh__chart_detector) > 10:
                summary_dict = {'zh_plot': summary}
                movie.update(summary_dict)
    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func, status=2,
                       imdb_id=imdb_id, msg=str(er))
    # other_names_row_list [{'imdb_id': 'tt1745960', 'lang_type': 'tw', 'movie_name': '捍衛戰士：獨行俠'},{'imdb_id': 'tt1745960', 'lang_type': 0, 'movie_name': '壯志凌雲2：馬弗裡克'} ...]

    # save to sql db
    try:
        if other_names_row_list:
            insert_dict_list_into_sql(sql_conn, 'other_names', other_names_row_list, ignore=True)
        if insert_movie_info != {}:
            update_multi_col_by_id(sql_conn, 'movie_info', insert_movie_info, 'imdb_id', imdb_id)
        if summary_dict != {}:
            update_one_col_by_id(sql_conn, 'plot', summary_dict, 'imdb_id', imdb_id)

        # fill tw_name field order: imdb default > tw > cn
        imdb_condition = " WHERE imdb_id='{id}'".format(id=imdb_id)
        find_result = sql_select_column('movie_info', ['tw_name'], sql_conn, condition=imdb_condition)
        if 'name' in movie and find_result == [(None,)]:
            if 'tw' in movie['name']:
                update_one_col_by_id(sql_conn, 'movie_info', {'tw_name': movie['name']['tw']}, 'imdb_id', imdb_id)
                update_one_col_by_id(sql_conn, 'movie_info', {'tw_name': movie['name']['tw']}, 'imdb_id', imdb_id)
            else:
                update_one_col_by_id(sql_conn, 'movie_info', {'tw_name': movie['name']['cn']}, 'imdb_id', imdb_id)
        mark_column(connection=sql_conn, table_name='imdb_movie_id', column_name='douban_03', status=1,
                    where_id='imdb_id', id_value=imdb_id)

    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func, status=2,
                       imdb_id=imdb_id, msg=str(er))

    # backup movie json into mongo
    try:
        insert_mongo_doc(mongo_db, 'douban_03_detail_json', movie)
        return 1

    except Exception as er:
        save_error_log(connection=sql_conn, start=start, file_name=file_name, func_name=func, status=2,
                       imdb_id=imdb_id, msg=str(er))
        print('backup movie json into mongodb failed')


def get_douban_03_id_list():
    engine = create_conn_pool()
    connection = get_connection(engine)
    # get imdb_id list
    query = "SELECT imdb_id FROM movie.imdb_movie_id WHERE douban_02 =1 and douban_03 =2;"
    out = connection.execute(query).fetchall()
    id_list = [i[0] for i in out]
    connection.close()
    engine.dispose()
    return id_list


def multi_thead_cleaning_mongo_douban_02_detail_page(start, end, worker_num):
    caller_file = os.path.basename(__file__)
    mongo_db, mongo_conn = create_mongo_connection()
    imdb_id_list = get_douban_03_id_list()
    docs = get_mongo_doc_by_date(mongo_db, start, end, 'douban_02_detail_page')
    mongo_conn.close()
    imdb_detail_cleaner = Mongo_multi_thread(id_list=imdb_id_list, worker_num=worker_num, func=clean_douban_02_detail_page_and_insert, file_name=caller_file)
    imdb_detail_cleaner.create_worker()
    mongo_conn.close()

# multi_thead_cleaning_mongo_douban_02_detail_page('2022-08-18', '2022-08-25', 1)
