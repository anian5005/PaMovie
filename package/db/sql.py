import mysql.connector
import sqlalchemy
from sqlalchemy.pool import QueuePool
from db_setting import connect_set
import inspect
import os
import time
from datetime import datetime
import sys
from package.general import get_current_taiwan_time_str, taiwan_time_converter, get_current_taiwan_datetime, timedelta_second_converter

config = connect_set.rds.set

def create_conn_pool():
    conn_str = "mysql+pymysql://local:{pwd}@{host}:3306/movie".format(pwd=config['password'], host=config['host'])
    engine = sqlalchemy.create_engine(conn_str, poolclass=QueuePool, pool_pre_ping=True)
    return engine


def get_connection(engine):
    connection = engine.connect()
    return connection


def init_db():

    return mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database='movie')

def get_cursor(connection, opt=False):
    dic_option = opt
    for i in range(1, 6):
        try:
            if i == 1:
                connection.ping(reconnect=True, attempts=3, delay=2)
                break
            else:
                connection = init_db()
                connection.ping(reconnect=True, attempts=3, delay=2)
                break
        except mysql.connector.Error as err:
            print('connection failed')

    return connection.cursor(buffered=True, dictionary=dic_option)


def rating_from_db(merge_id):
    connection = init_db()
    cursor = get_cursor(connection, opt=True)
    sql = "SELECT * FROM movie.rating WHERE merge_id = %s;"
    cursor.execute(sql, (merge_id,))
    out = cursor.fetchall()
    if out != []:
        rating = out[0]
        connection.commit()
        # print(rating)
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

# return 0 if insert data empty
def insert_dict_into_sql(connection, table_name, my_dict, ignore=None):
    if my_dict == {}:
        print('insert dict is empty')
        return 0
    # get table column names
    query_0 = "SHOW COLUMNS FROM {table}".format(table=table_name)
    table_info = connection.execute(query_0).fetchall()  # ('time', 'float', 'YES', '', None, ''),....]
    columns = {i[0] for i in table_info}

    # check my_dict's keys are all in columns
    safe_dict = {key: val for key, val in my_dict.items() if key in columns}

    # insert dict into table
    if ignore == True:
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
    connection.execute(query_1, list(safe_dict.values()))
    # connection.close()
    print('write SQL table "{table}" SAVED'.format(table=table_name))



#dict_list = [{'imdb_id': 'tt1745960', 'lang_type': 'tw', 'name': '捍衛戰士：獨行俠'}, ...]
# return 0 if insert data empty
def insert_dict_list_into_sql(connection, table_name, dict_list, ignore=None):
    if dict_list == []:
        print('insert list is empty')
        return 0

    # get table column names
    query_0 = "SHOW COLUMNS FROM {table}".format(table=table_name)
    table_info = connection.execute(query_0).fetchall()
    columns = {i[0] for i in table_info}

    # check my_dict's keys are all in columns
    safe_dict = {key: val for key, val in dict_list[0].items() if key in columns}
    # insert dict into table

    if ignore == True:
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
    connection.execute(query_1, list((i.values()) for i in dict_list))
    # connection.close()
    print('write SQL table "{table}" SAVED'.format(table=table_name))


# input: 'movie_info', {'imdb_id': 'tt0000001', 'published':'1894-03-10'}
# output: INSERT IGNORE INTO movie_info (imdb_id, published) VALUES (%s, %s)
def convert_dict_to_insert_query(connection, table_name, insert_container):
    # get table column names
    query_col = "SHOW COLUMNS FROM {table}".format(table=table_name)
    table_info = connection.execute(query_col).fetchall()
    columns = {i[0] for i in table_info}

    # check my_dict's keys are all in columns
    if type(insert_container) == list and len(insert_container) > 0:

        safe_dict = {key: val for key, val in insert_container[0].items() if key in columns}

    elif type(insert_container) == dict:
        safe_dict = {key: val for key, val in insert_container.items() if key in columns}
    else:
        print('insert_container type error')
        return None

    query = "INSERT IGNORE INTO {table} ({columns}) VALUES ({value_placeholders})".format(
        table=table_name,
        columns=", ".join(safe_dict.keys()),
        value_placeholders=str('%s, ' * len(safe_dict)).rstrip(' ,')
    )
    if type(insert_container) == list:
        values = list((i.values()) for i in insert_container)
    elif type(insert_container) == dict:
        values = [safe_dict.values()]
    else:
        print('insert_container type error')
        return
    return query, values
# convert_dict_to_insert_query('movie_info', {'imdb_id': 'tt0000001', 'published':'1894-03-10'})

# input = [ {'table': table, 'value': value}, {'table': table, 'value': value} ]
# not used
def insert_multi_table_with_transaction(connection, insert_stuff):
    with connection.begin():
        for item in insert_stuff:
            query, values = convert_dict_to_insert_query(connection, item['table'], item['values'])
            connection.execute(query, values)

def update_multi_col_by_id(connection, table, dict, id_type, id_str):
    if dict == {}:
        return
    update_col_str = ''
    for key, val in dict.items():
        temp = "{field}='{value}',".format(field=key, value=val)
        update_col_str = update_col_str + temp
    update_col_str = update_col_str.rstrip(',')

    query = "UPDATE {table} SET {update_str} WHERE {id_type}='{id}';".format(table=table, update_str=update_col_str, id_type=id_type, id=id_str)
    connection.execute(query)

def update_one_col_by_id(connection, table, dict, id_type, id_str):
    if dict == {}:
        return
    update_col_str = ''
    value = None
    for key, val in dict.items():
        update_col_str = "{field}=%s".format(field=key)
        value = val
    query = "UPDATE {table} SET {update_str} WHERE {id_type}='{id}';".format(table=table, update_str=update_col_str, id_type=id_type, id=id_str)
    connection.execute(query, (value,))

def mark_column(connection, table_name, column_name, status, where_id, id_value):
    sql = "UPDATE {table} SET {column} = %s WHERE {id} = %s;".format(table=table_name, column=column_name, id=where_id )
    connection.execute(sql, (status, id_value))
    # connection.close()

def sql_select_column(table, columns, connection, struct=None, condition=''):
    # create column_str e.g. genre_id, genre
    columns_str = ''
    for idx, col in enumerate(columns):
        columns_str = columns_str + col + ' ,'
    columns_str = columns_str.rstrip(' ,')
    query = "SELECT {columns} FROM {table}".format(table=table, columns=columns_str) + condition + ';'
    if struct == 'dict':
        out = connection.execute(query).mappings().fetchall()
    else:
        out = connection.execute(query).fetchall()

    if struct == 'list':
        out = [i[0] for i in out]
    return out

def get_finshed_log(start, status, result_count):
    fname = os.path.basename(__file__)
    func_name = inspect.stack()[0][3]
    start_date = taiwan_time_converter(start)
    end = get_current_taiwan_datetime()
    end_date = get_current_taiwan_time_str()
    spent = round(timedelta_second_converter(end - start), 2)
    log = {
        'file_name': fname,
        'func_name': func_name,
        'start': start_date,
        'end': end_date,
        'spent': spent,
        'status_code': status,
        'result_count': result_count
    }
    return log

def save_error_log(connection, start, file_name, func_name, status, imdb_id, msg):
    end = get_current_taiwan_datetime()
    current_time = taiwan_time_converter(end)
    spent = round(timedelta_second_converter(end - start), 2)
    exc_type, exc_obj, exc_tb = sys.exc_info()

    log = {
        'file_name': file_name,
        'func_name': func_name,
        'log_time': current_time,
        'imdb_id': imdb_id,
        'spent': spent,
        'status_code': status,
        'exc_type': str(exc_type),
        'line': str(exc_tb.tb_lineno),
        'msg': str(msg)
    }
    insert_dict_into_sql(connection=connection, table_name='error_log', my_dict=log, ignore=None)
    return log

def save_processing_log(connection, start, file_name, func_name, status, imdb_id):
    end = get_current_taiwan_datetime()
    current_time = taiwan_time_converter(end)
    spent = round(timedelta_second_converter(end - start), 2)
    log = {
        'file_name': file_name,
        'func_name': func_name,
        'log_time': current_time,
        'imdb_id': imdb_id,
        'spent': spent,
        'status_code': status,
    }
    insert_dict_into_sql(connection=connection, table_name='data_processing_log', my_dict=log, ignore=None)
    return log


def save_processing_finished_log(connection, start, file_name, func_name, finished_num):
    start_date = taiwan_time_converter(start)
    end = get_current_taiwan_datetime()
    end_date = taiwan_time_converter(end)
    spent = round(timedelta_second_converter(end - start), 2)
    if finished_num != 0:
        work_per_s = round(spent/finished_num, 1)
    else:
        work_per_s = 0
    log = {
        'file_name': file_name,
        'func_name': func_name,
        'start': start_date,
        'end': end_date,
        'spent': spent,
        'status_code': 1,
        'result_count': finished_num,
        'work_per_s': work_per_s
    }
    insert_dict_into_sql(connection=connection, table_name='processing_finished_log', my_dict=log)
    return log


def delete_row():
    engine = create_conn_pool()
    connection = get_connection(engine)
    query = 'DELETE FROM `processing_finished_log` WHERE file_name="douban_03.py";'
    connection.execute(query)
    connection.close()
    engine.dispose()
