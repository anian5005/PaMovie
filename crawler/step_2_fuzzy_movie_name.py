import mysql.connector
import re, time
from db_setting import connect_set
from pymongo import MongoClient
from datetime import datetime

config = connect_set.rds.set
mongo = connect_set.mongo.set

mongo_pwd = mongo['password']
mongo_connection = "mongodb+srv://fuzzy_user:{pwd}@cluster0.n9qoj.mongodb.net/?retryWrites=true&w=majority".format(pwd=mongo_pwd)
client = MongoClient(mongo_connection)
db = client.movie
mongo_col = db.fuzzy_input_substr

log_path = 'crawler.log'

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



def fuzzy_name(str):
    result_list = []
    original = str
    # print('original_name', original)

    # replace space
    full_str = original.replace('  ', ' ').lstrip(' ').rstrip(' ')
    result_list.append(full_str)

    # split comma
    comma = r':|：'
    if re.search(comma, full_str):
        # print('check', re.search(comma, full_str))
        split_str = re.split(comma, full_str)
        for str in split_str:
            str = str.lstrip(' ').rstrip(' ')
            result_list.append(str)

    # print(result_list)
    return result_list

# fuzzy_name('Mike Harry  &  Misha')
# fuzzy_name('WWE Royal Rumble: The Complete Anthology')

# input:  en_movie name from [movie.imdb_movie_id]
# out: fuzzy name list to MongoDb
def fuzzy_imdb(year=1):
    start = time.time()

    connection = init_db()
    cursor = get_cursor(connection, opt=True)
    sql = "SELECT * FROM movie.imdb_movie_id  WHERE type ='Movie' and year > %s;"
    cursor.execute(sql, (year,))

    connection.commit()
    movie_ids = cursor.fetchall()

    for idx, row in enumerate(movie_ids):
        # print(row)
        imdb_id = row['imdb_id']
        title = row['title']
        og_title = row['og_title']
        # print('row', row)
        # fuzzy(title)
        title_fuzzy_list = fuzzy_name(title)
        # fuzzy(og_title)

        og_title_fuzzy_list = []
        if og_title != None:
            og_title_fuzzy_list = fuzzy_name(og_title)

        merge_list = title_fuzzy_list + og_title_fuzzy_list
        row.update({'fuzzy': merge_list})

        # change dict key 'imdb_id' to '_id'
        # row.pop("imdb_id")
        row.update({'imdb_id': imdb_id})
        # row_list.append(row)


        # SAVE DATA TO MONGO DB
        try:
            db.fuzzy_input_substr.insert_one(row)
        except:
            pass

    end = time.time()
    with open(log_path, 'a', encoding='utf-8') as f:
        now = datetime.now()
        current_time = now.strftime('%Y-%m-%d %H:%M:%S')
        f.write(current_time + '\tfuzzy_imdeb\t' + '\twrite to mongoDB.fuzzy_input_substr successfully' + '\ttime\t' + str(end - start) + '\n')

    return


# fuzzy_imdb()

