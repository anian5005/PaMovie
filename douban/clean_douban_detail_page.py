from package.db.mongo import get_mongo_doc_by_id
from opencc import OpenCC
import json
import re
from package.db.sql import insert_dict_list_into_db, update_on_duplicate_key
from package.db.mongo import create_mongo_connection
import os
import inspect
from package.multi_thread import MultiThread
import html
import time
from package.db.sql import save_error_log, MySQL

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
# output container: {'tw': '捍卫战士：独行侠', 'more': ['壮志凌云独行侠', '壮志凌云：独行者', '壮志凌云2：马弗里克', 'TopGun2', 'TopGunMaverick']}
def get_other_name(imdb_id, name_list, container):
    # container = movie_names_dict
    insert_other_names_row_list = []
    for name in name_list:
        FIND = 0  # the count of found country name
        lang_mapping = {'(台)': 'tw', '(港)': 'hk', '(臺)': 'tw'}
        if FIND == 0:
            for key in lang_mapping:
                if key in name:
                    FIND = FIND + 1
                    clean_name = name.replace(key, '').strip()
                    clean_name = html.unescape(clean_name)
                    lang_type = lang_mapping[key]
                    container.update({lang_type: clean_name})  # {'tw': '捍卫战士：独行侠'}
                    insert_other_names_row_list.append({
                        'imdb_id': imdb_id,
                        'lang_type': lang_type,
                        'movie_name': clean_name})
                    break
        if FIND == 0:
            clean_name = html.unescape(name).strip()
            insert_other_names_row_list.append({'imdb_id': imdb_id, 'lang_type': 0, 'movie_name': clean_name})
    return insert_other_names_row_list


def clean_douban_detail_page_and_save(imdb_id, sql_conn, mongo_conn):
    sql = MySQL()
    # log_info
    log_info = {
        'connection': sql_conn,
        'start': time.time(),
        'file_name': file_name,
        'func_name': inspect.stack()[0][3],
    }

    # get raw data from mongodb
    try:
        docs = get_mongo_doc_by_id(mongo_conn, 'douban_02_detail_page', 'imdb_id', imdb_id)
        document = docs[0]
    except Exception as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        return 2

    # init 3 data containers
    movie_info = {'imdb_id': imdb_id, 'filming_loc': 0}  # filming_loc default 0
    #  {'imdb_id': 'tt0004022', 'published': '1914-11-01', 'filming_loc': ' 意大利', 'lang': ' 意大利語'}
    movie_names_dict = {}
    insert_other_names_row_list = []  # imdb_id, 'lang_type', 'movie_name'
    celebrity_list = []

    # Convert language from zh-CN to zh-TW
    cc = OpenCC('s2tw')
    douban_id = document['douban_id']
    # cn -> zh-tw char
    # get movie string data
    title = cc.convert(document['cn_movie_title'])
    str_json_data = cc.convert(document['html_json_data'])
    str_info = cc.convert(document['html_info'])
    str_summary = cc.convert(document['html_summary'])

    # movie cn_title
    title = html.unescape(str(title))
    title_zh_chart_detector = re.findall('[\u4e00-\u9fa5]', title)
    if len(title_zh_chart_detector) > 0:
        insert_other_names_row_list.append({'imdb_id': imdb_id, 'lang_type': 'cn', 'movie_name': title})
        movie_names_dict.update({'cn': title})

    # json data
    if str_json_data is '':  # raw data is empty, stop cleaning
        return 0
    else:
        str_json_data = str_json_data.replace('\\', '\\\\')
        # json.decoder.JSONDecodeError: Invalid \escape eg. {'filming_loc': ' 美國 / 中國大陸'}
        json_data = json.loads(str_json_data, strict=False)

    # datePublished
    for key in json_data:
        if key == 'datePublished':
            movie_info.update({'published': json_data[key]})

    # get clean_celebrity_list
    p_types = ['director', 'author', 'actor']
    p_safe_type = [i for i in p_types if i in json_data.keys()]
    for key in p_safe_type:
        person_list = json_data[key]
        if person_list:
            for person in person_list:
                url = person['url'].replace('\\\\', '\\')
                zh_name, en_name = split_name(person['name'])
                p_id = filter_douban_id(url)
                p_dict = {
                    'imdb_id': imdb_id,
                    'job_type': key,
                    'douban_id': douban_id,
                    'douban_per': p_id,
                    'zh_name': zh_name,
                    'en_name': en_name}

                celebrity_list.append(p_dict)

    # other_name, filming_loc, lang
    if str_info != '':
        movie_info_list = str_info.replace('  ', ' ').split('\n')
        for i in movie_info_list:
            if ':' in i:
                key = i.split(':')[0]
                value = i.split(':')[1]
                if key == '又名':
                    other_name_list = value.split('/')
                    # update other_names into container
                    insert_other_names_row_list = get_other_name(imdb_id, other_name_list, container=movie_names_dict)
                elif key == '製片國家/地區':
                    movie_info.update({'filming_loc': value})
                elif key == '語言':
                    movie_info.update({'lang': value})

    # summary
    try:
        if str_summary is not '':
            pass
        else:
            summary = str_summary.replace('\n', '').strip()
            zh_chart_detector = re.findall('[\u4e00-\u9fa5]', summary)
            if len(zh_chart_detector) > 5:
                movie_info.update({'zh_plot': summary})
    except Exception as er:
        save_error_log(status=2, imdb_id=imdb_id, msg=str(er), log_info=log_info)

    # fill tw_name field order: imdb default > tw > cn
    imdb_condition = " WHERE imdb_id='{id}'".format(id=imdb_id)
    find_result = sql.fetch_data(sql_conn, 'movie_info', ['tw_name'], struct='dict', condition=imdb_condition)
    if find_result == [{'tw_name': None}]:  # db tw_name is empty and get zh movie name, fill it with 'tw' or 'cn'
        if 'tw' in movie_names_dict:
            movie_info.update({'tw_name': movie_names_dict['tw']})
        elif 'cn' in movie_names_dict:
            movie_info.update({'tw_name': movie_names_dict['cn']})

    # save data to mysql
    try:
        if movie_info != {}:  # col: tw_name, filming_loc, lang'
            update_on_duplicate_key(sql_conn, 'movie_info', [movie_info], multi_thread=True)

        if celebrity_list:
            insert_dict_list_into_db(sql_conn, 'douban_staff', celebrity_list, ignore=True)
            insert_dict_list_into_db(sql_conn, 'douban_celebrity', celebrity_list, ignore=True)

        if insert_other_names_row_list:  # col: 'imdb', 'lang', 'name'
            insert_dict_list_into_db(sql_conn, 'other_names', insert_other_names_row_list, ignore=True)
        if 'zh_plot' in movie_info != {}:  # col: 'zh_plot'
            update_on_duplicate_key(sql_conn, 'plot', [movie_info], multi_thread=True)

    except Exception as er:
        save_error_log(status=3, imdb_id=imdb_id, msg=str(er), log_info=log_info)
        return 3

    return 1


def get_douban_03_id_list():
    sql = MySQL()
    engine, sql_conn = sql.get_connection()
    # get imdb_id list
    query = "SELECT imdb_id FROM movie.movie_info where img is not null and filming_loc is null;"
    out = sql_conn.execute(query).fetchall()
    id_list = [i[0] for i in out]
    sql_conn.close()
    engine.dispose()
    return id_list


def multi_thread_clean_douban_02_detail_page_and_save(worker_num):
    resource_list = ['sql_conn', 'mongo_conn']
    mongo_db, mongo_conn = create_mongo_connection()
    imdb_id_list = [{'imdb_id': i} for i in get_douban_03_id_list()]
    # imdb_id_list = ['tt0012769', "tt0006397"]
    douban_detail_cleaner = MultiThread(id_list=imdb_id_list,
                                        worker_num=worker_num,
                                        func=clean_douban_detail_page_and_save,
                                        file_name=file_name,
                                        table='',
                                        resource_list=resource_list)
    douban_detail_cleaner.create_worker()
    mongo_conn.close()


def testing_without_multi_thread():
    sql = MySQL()
    engine, connection = sql.get_connection()
    mongo_db, mongo_conn = create_mongo_connection()
    tool_box = {'mongo_conn':  mongo_conn, 'sql_conn': connection}
    clean_douban_detail_page_and_save('tt0004022', **tool_box)
    connection.close()
    engine.dispose()
