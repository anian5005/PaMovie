import mysql.connector
from crawler import step_2_fuzzy_movie_name
from random import randint
import time
from time import sleep
from db_setting import connect_set
from crawler.eye_01 import eye_01
from package.general import log_time


config = connect_set.rds.set
log_path = 'main.log'

def init_db():

    return mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database='movie')

def get_cursor(connection, opt=False):
    dic_option = opt
    try:
        connection.ping(reconnect=True, attempts=3, delay=2)
    except mysql.connector.Error as err:
        # reconnect your cursor as you did in __init__ or wherever
        connection = init_db()
    return connection.cursor(buffered=True, dictionary=dic_option)




def history_movie_db(year):
    file_path = 'time_log.text'
    func_name = 'history_movie_db'

    for i in range(1, 6):
        try:
            connection = init_db()
            cursor = get_cursor(connection, opt=True)
            break
        except Exception as e:
            if i == 5:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write(log_time() + '\t' + 'func: ' + func_name + '\t  Error Mesaage\t' + e + '\n')
                    return
            else:
                sleep(3)
                pass

    # get id list with eye_01 col null
    # SELECT * FROM movie.imdb_movie_id  WHERE type ='Movie' and year > %s AND scrape <=> NULL;
    sql = "SELECT * FROM movie.imdb_movie_id  WHERE type ='Movie' and year > %s and eye_01 <=> NULL;"
    cursor.execute(sql, (year,))
    connection.commit()
    movie_ids = cursor.fetchall()
    # print('movie_ids', movie_ids)

    all_time = 0

    for idx, id in enumerate(movie_ids):
        start = time.time()

        all_imdb_id = []
        # {'imdb_id': 'tt0439572', 'type': 'Movie', 'title': '閃電俠', 'og_title': 'the flash', 'year': '2023'}
        print('id', id)
        imdb_id = id['imdb_id']
        title = id['title']
        og_title = id['og_title']
        year = id['year']

        # Merge all zh_title or en_title to fuzzy
        if og_title != None:
            title_list = [title, og_title]
        else:
            title_list = [title]
        # Make each movie's fuzzy title list
        all_fuzzy_list = []
        for title in title_list:
            fuzzy_list = step_2_fuzzy_movie_name.fuzzy_name(title)
            all_fuzzy_list = all_fuzzy_list + fuzzy_list
        # print('all_fuzzy_list', all_fuzzy_list)  # ['閃電俠', 'the flash']


        for f_str in all_fuzzy_list:
            print('fuzzy_str', f_str)
            # search movie name, if match then save to sql DB main_info
            sleep(randint(0, 5))

            eye_01(imdb_id, f_str)
        end = time.time()
        spent = end - start

        all_time = all_time + spent
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write('idx ' + str(idx) + ' spent time ' + str(all_time) +'\n')

        #     # write and update movie data
        #     per_str_imdb_list = step_3_get_open_eye.search_by_eye(str)
        #     all_imdb_id = all_imdb_id + per_str_imdb_list
        #
        # if imdb_id not in all_imdb_id:
        #     connection = init_db()
        #     cursor = get_cursor(connection)
        #     sql = "UPDATE movie.imdb_movie_id SET scrape = %s WHERE imdb_id = %s;"
        #     cursor.execute(sql, (0, imdb_id))
        #     connection.commit()
        #     print('imdb_id', imdb_id, 'scrape NO RESULT', '(write_search_by_eye_result)')


history_movie_db('2020')