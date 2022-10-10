import sqlalchemy
from sqlalchemy.pool import QueuePool
from db_setting import connect_set
import sys
import html
import time
from datetime import datetime


class MySQL:
    def __init__(self):
        config = connect_set.rds.set
        self.conn_config = "mysql+pymysql://local:{pwd}@{host}:3306/movie".format(
            pwd=config['password'],
            host=config['host']
        )

    def create_conn_pool(self):
        engine = sqlalchemy.create_engine(self.conn_config, poolclass=QueuePool, pool_pre_ping=True, pool_size=30, max_overflow=5)
        return engine

    def get_connection(self):
        engine = self.create_conn_pool()
        connection = engine.connect()
        return engine, connection

    def fetch_data(self, conn, table, columns, condition='', struct=None):
        columns_str = ''
        for idx, col in enumerate(columns):
            columns_str = columns_str + col + ' ,'
        columns_str = columns_str.rstrip(' ,')

        query = "SELECT {columns} FROM {table} ".format(table=table, columns=columns_str) + condition + ';'
        if struct == 'dict':
            out = conn.execute(query).mappings().fetchall()
        else:
            out = conn.execute(query).fetchall()
            if struct == 'list':
                out = [i[0] for i in out]

        return out


# dict_list = [{'imdb_id': 'tt1745960', 'lang_type': 'tw', 'name': '捍衛戰士：獨行俠'}, ...]
# return 0 if inserted data is empty
def insert_dict_list_into_db(connection, table_name, dict_list, ignore=None):
    if not dict_list:
        return 0

    # get table column names
    query_0 = "SHOW COLUMNS FROM {table}".format(table=table_name)
    table_info = connection.execute(query_0).fetchall()
    columns = {i[0] for i in table_info}

    # check my_dict's keys are all in columns
    safe_dict = {key: val for key, val in dict_list[0].items() if key in columns}
    safe_dict_list = []
    for item in dict_list:
        checked_dict = {}
        for k, v in item.items():
            if k in safe_dict:
                checked_dict.update({k: v})
        safe_dict_list.append(checked_dict)

    # insert dict into table

    if ignore:
        ignore_option = ' IGNORE '
    else:
        ignore_option = ''

    query_1 = "INSERT {ignore_option} INTO {table} ({columns}) VALUES ({value_placeholders})".format(
        ignore_option=ignore_option,
        table=table_name,
        columns=", ".join(safe_dict.keys()),
        value_placeholders=str('%s, ' * len(safe_dict)).rstrip(' ,')
    )

    # close connection & engine
    connection.execute(query_1, list((i.values()) for i in safe_dict_list))
    print('write SQL table "{table}" SAVED'.format(table=table_name))


def update_on_duplicate_key(connection, table, data_list, multi_thread=None):
    query = ''
    values = []
    if not query:
        for data_dict in data_list:
            columns = ', '.join('`{0}`'.format(k) for k in data_dict)
            duplicates = ', '.join('{0}=VALUES({0})'.format(k) for k in data_dict)
            place_holders = ', '.join('%s'.format(k) for k in data_dict)
            query = "INSERT INTO {0} ({1}) VALUES ({2})".format(table, columns, place_holders)
            query = "{0} ON DUPLICATE KEY UPDATE {1}".format(query, duplicates)

            row_values = data_dict.values()
            values.append(row_values)
        connection.execute(query, values)
        print('data updated')
    if multi_thread is None:
        connection.close()


def save_error_log(status, imdb_id, msg, log_info, exc=True):
    connection = log_info['connection']
    start = log_info['start']
    file_name = log_info['file_name']
    func_name = log_info['func_name']

    end = time.time()
    spent = round((end - start), 0)
    exc_type, exc_obj, exc_tb = sys.exc_info()

    if exc is True:
        log = {
            'file_name': file_name,
            'func_name': func_name,
            'log_time': datetime.fromtimestamp(end),
            'imdb_id': imdb_id,
            'spent': spent,
            'status_code': status,
            'exc_type': str(exc_type),
            'line': str(exc_tb.tb_lineno),
            'msg': str(msg)
        }
    else:
        log = {
            'file_name': file_name,
            'func_name': func_name,
            'log_time': datetime.fromtimestamp(end),
            'imdb_id': imdb_id,
            'spent': spent,
            'status_code': status,
            'msg': str(msg)
        }
    insert_dict_list_into_db(connection=connection, table_name='error_log', dict_list=[log])
    return log


def save_processing_finished_log(finished_num, log_info):
    connection = log_info['connection']
    start = log_info['start']
    file_name = log_info['file_name']
    func_name = log_info['func_name']

    if 'status_code' in log_info:
        status_code = log_info['status_code']
    else:
        status_code = None

    if 'original_count' in log_info:
        original_count = log_info['original_count']
    else:
        original_count = None

    end = time.time()
    spent = round((end - start), 0)
    if finished_num != 0:
        work_per_s = round(spent/finished_num, 1)
    else:
        work_per_s = 0

    log = {
        'file_name': file_name,
        'func_name': func_name,
        'start': datetime.fromtimestamp(start),
        'end': datetime.fromtimestamp(end),
        'spent': spent,
        'status_code': status_code,
        'result_count': finished_num,
        'work_per_s': work_per_s,
        'original_count': original_count
    }
    insert_dict_list_into_db(connection=connection, table_name='processing_finished_log', dict_list=[log])

    return log


# website data model
def fetch_movie_detail_dict(movie_id):
    sql = MySQL()
    engine, sql_conn = sql.get_connection()
    basic_info = {}

    # get rating data
    query = "SELECT * FROM movie.movie_rating where imdb_id='{}'".format(movie_id)
    rating_tuple_list = sql_conn.execute(query).mappings().fetchall()
    new_rating_dict = {}
    if len(rating_tuple_list) > 0:
        rating_dict = rating_tuple_list[0]
        # convert mysql datetime to string e.g. datetime.datetime(2022, 10, 2, 0, 0)
        for i in rating_dict:
            if i in {'imdb_updated': 1, 'douban_updated': 1, 'tomatoes_updated': 1} and rating_dict[i] is not None:
                str_date = rating_dict[i].strftime('%Y-%m-%d')
                new_rating_dict.update({i: str_date})
            elif rating_dict[i] is not None:
                new_rating_dict.update({i: rating_dict[i]})
            elif rating_dict['tomato_votes'] is None:
                new_rating_dict.update({i: 'unknown'})
    basic_info.update({'rating': new_rating_dict})
    # {'imdb_id': 'tt1745960', 'imdb_rating': 8.4, 'imdb_votes': 382675, 'imdb_updated': '2022-10-02', 'tomatoes_rating': 96, 'tomato_votes': 447, 'tomatoes_source': 'crawler', 'tomatoes_updated': '2022-10-02'}

    # get basic info from db
    table = """
    (SELECT T3.*, genre_type.zh_genre from
    (SELECT * from
    (SELECT T1.*, movie_genre.genre_id FROM (SELECT * FROM movie.movie_info WHERE imdb_id = '{}') AS T1
    INNER JOIN movie.movie_genre
    ON movie.movie_genre.imdb_id = T1.imdb_id) AS T2) AS T3
    INNER JOIN movie.genre_type
    ON T3.genre_id = movie.genre_type.genre_id) AS T4
    """.format(movie_id)

    columns = ['T4.*', 'plot.zh_plot', 'plot.en_plot']
    condition = "LEFT JOIN movie.plot ON T4.imdb_id = movie.plot.imdb_id"
    result_dict_list = sql.fetch_data(sql_conn, table, columns, condition, 'dict')

    # get staff info from db
    staff_columns = ['movie.staff.job_type', 'movie.celebrity.en_name']
    staff_condition = """ INNER JOIN movie.celebrity
    ON movie.staff.imdb_per = movie.celebrity.imdb_per
    WHERE imdb_movie = '{}';""".format(movie_id)
    staff_dict_list = sql.fetch_data(sql_conn, 'staff', staff_columns, staff_condition, 'dict')

    # get other names from db
    other_name_condition = "WHERE imdb_id = '{}';".format(movie_id)
    other_name_list = sql.fetch_data(sql_conn, 'other_names', ['movie_name'], other_name_condition, 'list')
    key_mapping_word = {
        'lang': '語言',
        'filming_loc': '拍攝地',
        'zh_genre': '類型',
        'runtime': '片長',
        'start_year': '上映日',
        'en_plot': '英文劇情介紹',
        'zh_plot': '中文劇情介紹',
        'imdb_id': 'IMDb 編號',
        'tw_name': '片名',
        'primary_title': '外文名',
        'original_title': '原始片名',
        'is_adult': '成人片',
        'img': 'img',
        'genre_id': 'genre_id',
        'published': '出版日期'
    }
    # get genre str e.g. '動作, 劇情片'
    genres_str = ''
    if result_dict_list:
        result_dict = dict(result_dict_list[0])  # convert sqlalchemy.engine.row.RowMapping to dict
        if len(result_dict_list) == 1 and 'zh_genre' in result_dict:
            genres_str = result_dict['zh_genre']
        elif len(result_dict_list) > 1 and 'zh_genre' in result_dict:
            genres_str = ''
            for row_dict in result_dict_list:
                genres_str = genres_str + ', ' + row_dict['zh_genre']
            genres_str = genres_str.lstrip(', ')

        # get basic info
        for key, val in result_dict.items():
            if key == 'zh_genre':
                basic_info['類型'] = genres_str
            if key == 'img':
                if val == '0':
                    val = 'movie_default_large.png'
                basic_info['img'] = "https://s3-summer.s3.ap-southeast-1.amazonaws.com/movie/poster/" + val

            elif key == 'imdb_id' or key == 'genre_id':
                pass
            elif key == 'is_adult':
                if val == 0:
                    basic_info['成人片'] = '否'
                else:
                    basic_info['成人片'] = '是'

            elif result_dict[key] is None:
                pass
            else:
                new_key = key_mapping_word[key]
                basic_info[new_key] = html.unescape(str(val))

        if '片長' in basic_info:
            basic_info['片長'] = basic_info['片長'] + ' 分鐘 '

        if staff_dict_list:
            job_type_mapping_word = {
                'actor': '演員',
                'actress': '演員',
                'writer': '編劇',
                'director': '導演',
                'cinematographer': '攝影',
                'composer': '作曲',
                'producer': '製片',
                'editor': '剪輯',
                'archive_footage': 'archive footage'
            }
            staff_dict_list = [dict(item) for item in staff_dict_list]
            for idx, person_dict in enumerate(staff_dict_list):
                key = person_dict['job_type']
                if key in job_type_mapping_word:
                    new_val = job_type_mapping_word[key]
                    staff_dict_list[idx]['job_type'] = new_val
            basic_info['工作人員'] = staff_dict_list

        # merge dict
        if other_name_list:
            basic_info['又名'] = other_name_list
        sql_conn.close()

        return basic_info
    else:
        return None


def fetch_movie_home_page_list(sql_conn):
    sql = MySQL()
    table = "movie_info"
    columns = ['imdb_id', 'tw_name', 'primary_title', 'start_year', 'img']
    condition = " WHERE start_year > 2018;"
    result_dict_list = sql.fetch_data(sql_conn, table, columns, condition, 'dict')
    sql_conn.close()
    return result_dict_list


# website data model
def rating_from_db(merge_id):
    sql = MySQL()
    engine, connection = sql.get_connection()
    sql = "SELECT * FROM movie.rating WHERE merge_id = %s;"
    out = connection.execute(sql, (merge_id,)).fetchall()

    if out:
        rating = out[0]
        return rating
    else:
        return {'merge_id': merge_id,
                'imdb_id': None,
                'imdb_score': 0,
                'imdb_count': 0,
                'tomato_meter': 0,
                'tomato_audience': 0,
                'tomato_review_count': 0,
                'tomato_audience_count': 0,
                'meta_score': 0,
                'meta_score_count': 0,
                'meta_user_score': 0,
                'meta_user_count': 0,
                'yahoo_score': 0,
                'yahoo_count': 0
                }


def recheck_if_error(sql_conn, date, func_name):
    query = "SELECT log_id FROM movie.error_log WHERE log_time >= '{}' AND func_name ='{}';".format(date, func_name)
    result_list = sql_conn.execute(query).fetchall()
    return len(result_list)


def get_dashboard_data():
    sql = MySQL()
    engine, sql_conn = sql.get_connection()

    # movie data pipeline
    movie_data_result_dict_list = sql.fetch_data(sql_conn, 'dashboard_movie_count', ['*'], '', 'dict')
    data_date_list = []
    imdb_count_list = []
    douban_count_list = []
    rotten_tomatoes_count_list = []

    merge_data_table_row_list = []
    merge_rating_table_row_list = []

    id_current_count_list = []

    imdb_new_list = []
    douban_new_list = []
    tomato_new_list = []

    movie_update_time_list = []
    imdb_data_update_time_speed = []
    douban_data_update_time_speed = []
    tomato_id_update_time_speed = []

    imdb_douban_conversion_rate_list = []
    douban_tomato_conversion_rate_list = []

    for idx, item in enumerate(movie_data_result_dict_list):
        pipeline_result_row_list = []

        date = item['log_time'].strftime("%Y-%m-%d")
        imdb_old_count = item['imdb_old_count']
        imdb_id_count = item['imdb_id_count']
        douban_old_count = item['douban_old_count']
        douban_id_count = item['douban_id_count']
        rotten_tomatoes_old_count = item['rotten_tomatoes_old_count']
        rotten_tomatoes_id_count = item['rotten_tomatoes_id_count']
        douban_new_count = item['douban_new_count']
        imdb_new_count = item['imdb_new_count']
        rotten_tomatoes_new_count = item['rotten_tomatoes_new_count']

        douban_data_time = item['douban_data_time']
        tomato_id_time = item['tomato_id_time']
        imdb_data_time = item['imdb_data_time']

        imdb_douban_conversion_rate = round(douban_id_count / imdb_id_count, 4)
        douban_tomato_conversion_rate = round(rotten_tomatoes_id_count / douban_id_count, 4)
        movie_update_time = imdb_data_time + tomato_id_time + douban_data_time

        if idx == len(movie_data_result_dict_list) - 1:
            id_current_count_list = [imdb_id_count, douban_id_count, rotten_tomatoes_id_count]

        data_date_list.append(date)
        imdb_count_list.append(imdb_id_count)
        douban_count_list.append(douban_id_count)
        movie_update_time_list.append(movie_update_time)

        imdb_new_list.append(imdb_new_count)
        douban_new_list.append(douban_new_count)
        tomato_new_list.append(rotten_tomatoes_new_count)

        rotten_tomatoes_count_list.append(rotten_tomatoes_id_count)
        imdb_douban_conversion_rate_list.append(imdb_douban_conversion_rate)
        douban_tomato_conversion_rate_list.append(douban_tomato_conversion_rate)

        # movie data date table
        pipeline_result_row_list.append(date)
        if imdb_old_count + imdb_new_count == imdb_id_count:
            pipeline_result_row_list.append('成功')
        else:
            pipeline_result_row_list.append('失敗')
        if douban_old_count + douban_new_count == douban_id_count:
            pipeline_result_row_list.append('成功')
        else:
            pipeline_result_row_list.append('失敗')
        if rotten_tomatoes_old_count + rotten_tomatoes_new_count == rotten_tomatoes_id_count:
            pipeline_result_row_list.append('成功')
        else:
            pipeline_result_row_list.append('失敗')

        if imdb_new_count != 0:
            imdb_data_update_time_speed.append(round(imdb_data_time / imdb_new_count, 2))
        else:
            imdb_data_update_time_speed.append(0)

        if douban_new_count != 0:
            douban_data_update_time_speed.append(round(douban_data_time / douban_new_count, 2))
        else:
            douban_data_update_time_speed.append(0)

        if rotten_tomatoes_new_count != 0:
            tomato_id_update_time_speed.append(round(tomato_id_time / rotten_tomatoes_new_count, 2))
        else:
            tomato_id_update_time_speed.append(0)

        merge_data_table_row_list.append(pipeline_result_row_list)

    movie_total_count_dict = {
        'date_list': data_date_list,
        'imdb_id_count_list': imdb_count_list,
        'douban_id_count_list': douban_count_list,
        'rotten_tomatoes_count_list': rotten_tomatoes_count_list
    }

    movie_data_speed_dict = {
        'imdb_data_update_speed': imdb_data_update_time_speed,
        'douban_data_update_speed': douban_data_update_time_speed,
        'tomato_id_update_time_speed': tomato_id_update_time_speed

    }

    new_movie_dict = {
        'imdb_new_list': imdb_new_list,
        'douban_new_list': douban_new_list,
        'tomato_new_list': tomato_new_list
    }

    conversion_rate_dict = {
        'imdb_douban': imdb_douban_conversion_rate_list,
        'douban_tomato': douban_tomato_conversion_rate_list
    }
    movie_rating_result_dict_list = sql.fetch_data(sql_conn, 'dashboard_rating_count', ['*'], '', 'dict')

    # rating data pipeline
    rating_date_list = []

    imdb_updated_count_list = []
    douban_updated_count_list = []
    rotten_tomatoes_updated_count_list = []
    rotten_tomatoes_rating_time_list = []
    tomato_no_rating_count_list = []
    rotten_tomatoes_error_count_list = []
    tomato_rating_successfully_count_list = []
    douban_no_rating_count_list = []
    douban_rating_successfully_count_list = []
    douban_block_count_list = []

    douan_rating_speed_list = []
    tomato_rating_speed_list = []

    imdb_rating_time_list = []
    douban_rating_time_list = []
    tomato_omdb_count_list = []
    douban_error_count_list = []
    imdb_no_rating_count_list = []

    for item in movie_rating_result_dict_list:
        pipeline_result_row_list = []

        rating_date = item['log_time'].strftime("%Y-%m-%d")
        imdb_updated_count = item['imdb_updated_count']
        imdb_rating_time = item['imdb_rating_time']
        imdb_error_count = item['imdb_error_count']
        imdb_no_rating_count = item['imdb_no_rating_count']

        douban_to_update_count = item['douban_to_update']
        douban_updated_count = item['douban_updated_count']
        douban_error_count = item['douban_error_count']
        douban_no_rating_count = item['douban_no_rating_count']
        douban_rating_time = item['douban_rating_time']

        rotten_tomatoes_updated_count = item['rotten_tomatoes_updated_count']
        rotten_tomatoes_rating_time = item['rotten_tomatoes_rating_time']
        rotten_tomatoes_error_count = item['rotten_tomatoes_error_count']
        tomato_no_rating_count = item['tomato_no_rating_count']
        tomato_omdb_count = item['tomato_omdb_count']

        pipeline_result_row_list.append(rating_date)
        pipeline_result_row_list.append(imdb_error_count)
        pipeline_result_row_list.append(douban_error_count)
        pipeline_result_row_list.append(rotten_tomatoes_error_count)
        merge_rating_table_row_list.append(pipeline_result_row_list)

        rating_date_list.append(rating_date)

        imdb_updated_count_list.append(imdb_updated_count)
        imdb_no_rating_count_list.append(imdb_no_rating_count)

        douban_updated_count_list.append(douban_updated_count)
        douban_error_count_list.append(douban_error_count)
        douban_no_rating_count_list.append(douban_no_rating_count)
        douban_rating_successfully_count_list.append(douban_updated_count - douban_no_rating_count)
        douban_block_count_list.append(douban_to_update_count - douban_updated_count)
        douan_rating_speed_list.append(round(douban_rating_time / douban_updated_count, 4))

        rotten_tomatoes_updated_count_list.append(rotten_tomatoes_updated_count)
        imdb_rating_time_list.append(imdb_rating_time)
        douban_rating_time_list.append(douban_rating_time)

        rotten_tomatoes_rating_time_list.append(rotten_tomatoes_rating_time)
        tomato_no_rating_count_list.append(tomato_no_rating_count)
        rotten_tomatoes_error_count_list.append(rotten_tomatoes_error_count)
        tomato_omdb_count_list.append(tomato_omdb_count)
        tomato_rating_successfully_count_list.append(
            rotten_tomatoes_updated_count - tomato_no_rating_count - rotten_tomatoes_error_count)
        tomato_crawler_and_omdb_rating_speed = round(
            rotten_tomatoes_rating_time / (tomato_omdb_count + rotten_tomatoes_updated_count), 4)
        tomato_rating_speed_list.append(tomato_crawler_and_omdb_rating_speed)

    # total count
    img_comdition = " where start_year > 2017 and img is not null and img !=0"
    img_list = sql.fetch_data(sql_conn, 'movie_info', ['imdb_id'], img_comdition, 'list')
    movie_rating_count = imdb_updated_count_list[-1] + douban_updated_count_list[-1] + \
                         rotten_tomatoes_updated_count_list[-1] - douban_no_rating_count_list[-1] - \
                         tomato_no_rating_count_list[-1]
    movie_rating_time = imdb_rating_time_list[-1] + douban_rating_time_list[-1] + rotten_tomatoes_rating_time_list[-1]
    total_new_data = imdb_new_list[-1] + douban_new_list[-1] + tomato_new_list[-1]
    total_header = {
        'data_count': douban_count_list[-1],
        'data_time': round(movie_update_time_list[-1] / total_new_data, 1),
        'movie_poster': len(img_list),
        'movie_rating_count': movie_rating_count,
        'rating_time': round(movie_rating_time / movie_rating_count, 2)
    }

    tomato_rating_count = {
        'rotten_tomatoes_error_count_list': rotten_tomatoes_error_count_list,
        'tomato_no_rating_count_list': tomato_no_rating_count_list,
        'tomato_rating_successfully_count_list': tomato_rating_successfully_count_list,
        'tomato_omdb_count_list': tomato_omdb_count_list
    }

    imdb_rating_count = {
        'imdb_updated_count': imdb_updated_count_list,
        'imdb_no_rating_count': imdb_no_rating_count_list
    }

    douban_rating_count = {
        'douban_rating_successfully_count': douban_rating_successfully_count_list,
        'douban_error_count': douban_error_count_list,
        'douban_no_rating_count': douban_no_rating_count_list,
        'douban_block_count': douban_block_count_list
    }

    my_dict = {
        'date_list': data_date_list,
        'rating_date': rating_date_list,
        'movie_total_count_dict': movie_total_count_dict,
        'movie_data_speed_dict': movie_data_speed_dict,
        'conversion_rate': conversion_rate_dict,
        'new_add_count': new_movie_dict,
        'total_header': total_header,
        'tomato_rating_count': tomato_rating_count,
        'imdb_rating_count': imdb_rating_count,
        'douban_rating_count': douban_rating_count,
        'douan_rating_speed': douan_rating_speed_list,
        'tomato_rating_speed': tomato_rating_speed_list,
        'data_table_result': merge_data_table_row_list,
        'rating_table_result': merge_rating_table_row_list,
        'id_current_count': id_current_count_list
    }
    sql_conn.close()
    engine.dispose()

    return my_dict
