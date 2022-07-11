from pymongo import MongoClient
from db_setting import connect_set
from datetime import datetime
from pymongo import ASCENDING, DESCENDING,TEXT

mongo = connect_set.mongo.set

mongo_pwd = mongo['password']
mongo_connection = "mongodb+srv://fuzzy_user:{pwd}@cluster0.n9qoj.mongodb.net/?retryWrites=true&w=majority".format(pwd=mongo_pwd)
client = MongoClient(mongo_connection)
db = client.movie
# mongo_col = db.fuzzy_input_substr


log_path = "mongo.log"



# SAVE DATA TO MONGO DB eye_01_pages
def insert_eye_search_result(doc):
    db.eye_01_pages.insert_one(doc)
    print('mongo eye_01 insert successfully')


def create_unique_index(index_name):
    try:

        db.eye_01_pages.create_index(
            [("keyword",  TEXT)],
            unique=True, name=index_name
        )
    except Exception as er:
        print(er)

# create_multi_unique_index('imdb_keyword')

def get_mongo_eye_1(start, end):
    # $gte: greater than or equal to start_date
    # $lte: less than or equal to end_date
    mongo_col = db.eye_01_pages
    eye_1_docs = mongo_col.find({'created_date': {'$gte': start, '$lte': end}})
    # for i in eye_1_docs:
    #     print(i)

    return eye_1_docs

# get_mongo_eye_1('2022-07-05', '2022-07-05')


def insert_mongo_eye_03_movie_page(doc):
    # SAVE DATA TO MONGO DB
    func_name = 'insert_eye_03_movie_page'
    try:
        db.eye_03_detail.insert_one(doc)
        print(func_name + '\tmongo insert successfully\n')
    except:
        with open(log_path, 'a', encoding='utf-8') as f:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
            f.write(current_time + '\t' + func_name + '\tinsert failed\n')


def get_mongo_eye_03_detail(start, end):
    # $gte: greater than or equal to start_date
    # $lte: less than or equal to end_date
    mongo_col = db.eye_03_detail
    eye_3_docs = mongo_col.find({'created_date': {'$gte': start, '$lte': end}})
    # for i in eye_3_docs:
    #     print(i)
    #     print()
    return eye_3_docs

# get_mongo_eye_03_detail('2022-07-06', '2022-07-06')