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


# id_type : 'eye_id' or 'imdb_id'
# input: 'SAMTLP1893', 'eye_id'
# output: 1
def search_person_id(eye_id, id_type):
    sql = ("SELECT person_id FROM movie.star WHERE {} = %s;".format( id_type) )
    connection = init_db()
    cursor = get_cursor(connection)
    cursor.execute(sql, (eye_id,))
    connection.commit()
    result_list = cursor.fetchall()  # [(1,)]
    person_id = result_list[0][0]
    return person_id

def all_zh_data_from_db():
    # create table
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
    # create table
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
    # create table
    connection = init_db()
    cursor = get_cursor(connection, opt=True)
    sql = "SELECT merge_id, eye_id, en_title, zh_title, runtime, release_date, video, image, text FROM movie.main_info WHERE merge_id = %s"
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

# rating_from_db('tt1745960')


# insert zh_open_eye_data into table main_info & zh_star % cast
def write_main_info(dict):
    # Create DB movie
    connection = init_db()
    cursor = get_cursor(connection)
    cursor.execute("CREATE DATABASE IF NOT EXISTS movie")
    connection.commit()

    # Create table movie.main_info
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS main_info ("
                   "`merge_id` varchar(255) PRIMARY KEY,"
                  "`imdb_id` varchar(255),"
                   "`eye_id` char(20),"
                   "`en_title` varchar(255),"
                   "`zh_title` varchar(255),"
                   "`runtime` int,"
                   "`release_date` char(12),"
                   "`video` varchar(255),"
                   "`image` varchar(255),"
                   "`text` MEDIUMTEXT)"
                   )
    connection.commit()

    # Create table star
    cursor = get_cursor(connection)
    # eye_id, zh_name, imdb_id, en_name
    cursor.execute("CREATE TABLE IF NOT EXISTS star ("
                   "`person_id` int AUTO_INCREMENT PRIMARY KEY,"
                   "`eye_id` varchar(15) UNIQUE,"
                   "`zh_name` varchar(255),"
                   "`imdb_id` int unsigned UNIQUE,"
                   "`en_name` varchar(255))"
                   )
    connection.commit()

    # Create table movie.cast
    cursor = get_cursor(connection)
    # merge_id, type, person_id
    cursor.execute("CREATE TABLE IF NOT EXISTS cast ("
                   "`merge_id`varchar(255),"
                   "`type` CHAR(4),"
                   "`person_id` int UNIQUE NOT NULL)",
                   "UNIQUE KEY `cast_index`(`movie_id`,'type', `person_id`) )"
                   )
    connection.commit()

    # Extract value from input
    row = dict
    merge_id = row['merge_id']
    imdb_id = row['imdb_id']
    eye_id = row['eye_id']
    en_title = row['en_title']
    zh_title = row['zh_title']
    runtime = row['runtime']
    release_date = row['release_date']
    video = row['video']
    image = row['image_url']
    text = row['text']
    director = row.get('director', None)
    writer = row.get('writer', None)
    stars = row['star_dict']  # [('GR0071597', '高山南'), ('004000479', '山口勝平'), ('PI0071596', '山崎和佳奈'), ('XA0093172', '小山力也')]

    star_id_list = [tuple[0] for tuple in stars]  # ['GR0071597', '004000479', 'PI0071596', 'XA0093172']

    # Inser stars into DB table star
    stars_tuples = []  # for star table
    cast_tuples = []  # for cast table
    movie_id = merge_id

    if director != None:
        stars_tuples.append(director)
    if writer != None:
        stars_tuples.append(writer)
    if stars != []:
        stars_tuples = stars_tuples + stars

    cursor = get_cursor(connection)
    sql = "INSERT IGNORE INTO star (eye_id, zh_name) VALUES (%s, %s)"
    cursor.executemany(sql, stars_tuples)
    connection.commit()

    # Inser star & director % writer into movie.cast
    # ABRV : director-> dir, writer-> wrtr
    star_cast_tuples = []
    if director != None:
        director_person_id = search_person_id(director[0], 'eye_id')
        director_tuple = (movie_id, 'dir', director_person_id)
        cast_tuples.append(director_tuple)
    if writer != None:
        writer_person_id = search_person_id(writer[0], 'eye_id')
        writer_tuple = (movie_id, 'wrtr', writer_person_id)
        cast_tuples.append(writer_tuple)
    if stars != []:
        # stars : [('CE0026830', '克里斯伊凡'), ('UZ0089426', '琪琪帕瑪'),..]
        for star in stars:
            star_id = star[0]
            star_person_id = search_person_id(star_id, 'eye_id')
            star_tuple = (merge_id, 'star', star_person_id)
            star_cast_tuples.append(star_tuple)

        cast_tuples = cast_tuples + star_cast_tuples

    cursor = get_cursor(connection)
    sql = "INSERT IGNORE INTO cast (merge_id, type, person_id) VALUES (%s, %s, %s)"
    cursor.executemany(sql, cast_tuples)
    connection.commit()

    # Inser movie data into DB table main_info
    cursor = get_cursor(connection)
    tuple = (merge_id, imdb_id, eye_id, zh_title, en_title,  runtime, release_date, image, video, text)
    sql = "INSERT IGNORE INTO main_info(merge_id, imdb_id, eye_id, zh_title, en_title,  runtime, release_date, image, video, text) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple)
    connection.commit()
    return



def write_search_by_eye_result(tuple):
    # create table
    connection = init_db()
    cursor = get_cursor(connection)
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS movie.search_by_eye_result ("
        "`search_word` varchar(255),"
        "`eye_id` varchar(20) PRIMARY KEY,"
        "`year` int,"
        "`imdb_id` varchar(20),"
        "`zh_name` varchar(255),"
        "`en_name` varchar(255),"
        "`scrape` int)"
                   )
    connection.commit()

    # keyword, eye_id,   zh_name, en_name, imdb_id, year
    cursor = get_cursor(connection)

    print(' before insert tuple', tuple)
    sql = "INSERT IGNORE INTO search_by_eye_result (search_word, eye_id, zh_name, en_name, imdb_id, year) VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple)
    connection.commit()

    #  note imdb_id had be scraped

    imdb_id = tuple[4]
    connection = init_db()
    cursor = get_cursor(connection)
    sql = "UPDATE movie.imdb_movie_id SET scrape = %s WHERE imdb_id = %s;"
    cursor.execute(sql, (1, imdb_id))
    connection.commit()
    print('imdb_id', imdb_id, 'record scrape', '(write_search_by_eye_result)')
    return

<<<<<<< HEAD
# write_search_by_eye_result( ('Shut In', 'fEatm0884089', '大開眼戒', 'Eyes Wide Shut', 'tt0120663', '1999') )
=======
# write_search_by_eye_result( ('Shut In', 'fEatm0884089', '大開眼戒', 'Eyes Wide Shut', 'tt0120663', '1999') )


def write_eye_01(row):
    connection = init_db()
    cursor = get_cursor(connection)
    sql = "INSERT IGNORE INTO eye_01 (id, date, time, imdb_id, word, result_count) VALUES (%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, row)
    connection.commit()
    print('write_eye_01 : sql save')


def mark_eye_01(status, imdb_id):
    connection = init_db()
    cursor = get_cursor(connection)
    sql = "UPDATE movie.imdb_movie_id SET eye_01 = %s WHERE imdb_id = %s;"
    cursor.execute(sql, (status , imdb_id))
    connection.commit()
    print('imdb_id', imdb_id, 'record eye_01', 'status', status)


def mark_eye_02(status, eye_id):
    connection = init_db()
    cursor = get_cursor(connection)
    sql = "UPDATE movie.eye_02_id SET scrape = %s WHERE eye_id = %s;"
    cursor.execute(sql, (status, eye_id))
    connection.commit()
    print('eye_id', eye_id, 'record eye_02_id', 'status', status, 'finished')


def write_eye_02_id(tuple_list):
    connection = init_db()
    cursor = get_cursor(connection)
    sql = "INSERT IGNORE INTO eye_02_id (mongo_start, mongo_end, eye_id) VALUES (%s,%s,%s)"
    cursor.executemany(sql, tuple_list)
    connection.commit()
    print('write_eye_02_id : SQL SAVE')

def get_eye_id_from_sql(start_date, end_date):
    # create table
    connection = init_db()
    cursor = get_cursor(connection, opt=False)
    date_range = "WHERE (mongo_end BETWEEN '{min}' AND '{max}');".format(min=start_date, max=end_date)
    sql = "SELECT eye_id FROM movie.eye_02_id " + date_range
    cursor.execute(sql)
    out = cursor.fetchall()
    eye_id_list = [id[0] for id in out]  # ['A12020090204', 'f1en00326716',...]
    # print(eye_id_list)
    return eye_id_list


# get_eye_id_from_sql('2022-07-05', '2022-07-05')

>>>>>>> sprint_2
