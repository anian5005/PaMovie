import mysql.connector
from db_setting import connect_set
config = connect_set.rds.set
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

def all_zh_data_from_db():
    connection = init_db()
    cursor = get_cursor(connection, opt=True)
    sql = "SELECT merge_id, en_title, zh_title, image,release_date FROM movie.main_info"
    cursor.execute(sql)
    out = cursor.fetchall()
    if out != []:
        main = out
        connection.commit()
        # print(main)
        return main
    else:
        return None
# all_zh_data_from_db()


def filter_zh_data_from_db(year_min, year_max):
    connection = init_db()
    cursor = get_cursor(connection,opt=True)
    year_sql = "WHERE (release_date BETWEEN {min} AND {max});".format(min=year_min, max=year_max)
    sql = "SELECT merge_id, en_title, zh_title, image,release_date FROM movie.main_info " + year_sql
    # SELECT merge_id, en_title, zh_title, image,release_date FROM movie.main_info WHERE (release_date BETWEEN 2022 AND 2025);

    cursor.execute(sql)
    out = cursor.fetchall()
    if out != []:
        main = out
        connection.commit()
        # print(main)
        return main
    else:
        return None
# filter_zh_data_from_db(2022,2025)

def zh_data_from_db(merge_id):
    connection = init_db()
    cursor = get_cursor(connection, opt=True)
    sql = """SELECT 
                main_info.merge_id, 
                eye_id, 
                en_title, 
                zh_title, 
                runtime, 
                release_date, 
                video, image, 
                imdb_score, 
                imdb_count,
                tomato_meter,
                tomato_audience,
                tomato_review_count,
                tomato_audience_count,
                meta_score,
                meta_score_count,
                meta_user_score,
                meta_user_count,
                yahoo_count,
                yahoo_score
                FROM movie.rating INNER JOIN movie.main_info ON movie.main_info.merge_id = movie.rating.merge_id where main_info.merge_id = %s;"""
    cursor.execute(sql, (merge_id,))
    out = cursor.fetchall()
    if out != []:
        main = out[0]
        connection.commit()
        # print(main)
        return main
    else:
        return None
# zh_data_from_db()

def movie_page_data(merge_id):
    connection = init_db()
    cursor = get_cursor(connection, opt=True)
    sql = """SELECT *
                FROM movie.rating 
                LEFT JOIN movie.main_info ON movie.main_info.merge_id = movie.rating.merge_id
                LEFT JOIN movie.cast ON cast.merge_id = main_info.merge_id
                LEFT JOIN movie.star ON cast.person_id = star.person_id
                where main_info.merge_id = %s;"""
    cursor.execute(sql, (merge_id,))
    row_list = cursor.fetchall()
    connection.commit()

    # movie main info
    general = row_list[0].copy()
    for k in ['text', 'person_id', 'zh_name', 'type', 'en_name']:
        general.pop(k, None)
    # print(general)
    # movie cast
    cast_dict = {}

    for staff in row_list:
        type = staff['type']
        name = staff['zh_name']
        # print('staff', staff)
        if type =='dir':
            if cast_dict.get('dir', None) != None:
                cast_dict['dir'].append(name)
            else:
                cast_dict['dir']= [name]

        if type =='star':
            if cast_dict.get('star', None) != None:
                cast_dict['star'].append(name)
            else:
                cast_dict['star'] = [name]

        if type =='wrtr':
            if cast_dict.get('wrtr', None) != None:
                cast_dict['wrtr'].append(name)
            else:
                cast_dict['wrtr'] = [name]

    return general, cast_dict


# output: {'dir': ['喬瑟夫柯辛斯基'], 'star': ['湯姆克魯斯', '麥爾斯泰勒', '方基墨', '珍妮佛康納莉', '喬漢姆', '路易斯普曼', '格蘭鮑威爾', '摩妮卡巴巴羅']}
def zh_cast_from_db(merge_id):
    connection = init_db()
    cursor = get_cursor(connection, opt=True)
    sql = "SELECT type, zh_name FROM movie.star INNER JOIN movie.cast ON movie.cast.person_id = movie.star.person_id where merge_id = %s;"
    cursor.execute(sql, (merge_id,))
    cast = cursor.fetchall()
    connection.commit()

    cast_dict = {}

    for staff in cast:
        type = staff['type']
        name = staff['zh_name']
        # print('staff', staff)
        if type =='dir':
            if cast_dict.get('dir', None) != None:
                cast_dict['dir'].append(name)
            else:
                cast_dict['dir']= [name]

        if type =='star':
            if cast_dict.get('star', None) != None:
                cast_dict['star'].append(name)
            else:
                cast_dict['star'] = [name]

        if type =='wrtr':
            if cast_dict.get('wrtr', None) != None:
                cast_dict['wrtr'].append(name)
            else:
                cast_dict['wrtr'] = [name]

    # print('cast_dict', cast_dict)
    return cast_dict
# zh_cast_from_db('tt1745960')