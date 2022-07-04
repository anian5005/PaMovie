import mysql.connector
from crawler import step_2_fuzzy_movie_name
from crawler import step_3_get_open_eye
from random import randint
from time import sleep
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
    try:
        connection.ping(reconnect=True, attempts=3, delay=2)
    except mysql.connector.Error as err:
        # reconnect your cursor as you did in __init__ or wherever
        connection = init_db()
    return connection.cursor(buffered=True, dictionary=dic_option)




def history_movie_db(year):
    connection = init_db()
    cursor = get_cursor(connection, opt=True)

    # get id list
    sql = "SELECT * FROM movie.imdb_movie_id  WHERE type ='Movie' and year > %s AND scrape <=> NULL;"
    cursor.execute(sql, (year,))
    connection.commit()
    movie_ids = cursor.fetchall()
    # print('movie_ids', movie_ids)

    for id in movie_ids:

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


        for str in all_fuzzy_list:
            print('fuzzy_str', str)
            # search movie name, if match then save to sql DB main_info
            sleep(randint(0, 5))

            # write and update movie data
            per_str_imdb_list = step_3_get_open_eye.search_by_eye(str)
            all_imdb_id = all_imdb_id + per_str_imdb_list

        if imdb_id not in all_imdb_id:
            connection = init_db()
            cursor = get_cursor(connection)
            sql = "UPDATE movie.imdb_movie_id SET scrape = %s WHERE imdb_id = %s;"
            cursor.execute(sql, (0, imdb_id))
            connection.commit()
            print('imdb_id', imdb_id, 'scrape NO RESULT', '(write_search_by_eye_result)')


history_movie_db('2020')