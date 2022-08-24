from pymongo.write_concern import WriteConcern
from pymongo import MongoClient
from db_setting import connect_set
from pymongo import TEXT

mongo = connect_set.mongo.set

mongo_pwd = mongo['password']
aws = mongo['aws']
# mongo_connection = 'mongodb://local:{pwd}@{aws}:27017/movie'.format(pwd=mongo_pwd, aws=aws)
# client = MongoClient(mongo_connection)
# db = client.movie

def create_mongo_connection():
    mongo_conn_str = 'mongodb://local:{pwd}@{aws}:27017/movie'.format(pwd=mongo_pwd, aws=aws)
    mongo_connection = MongoClient(mongo_conn_str)
    mongo_db = mongo_connection.movie
    return mongo_db, mongo_connection

def create_unique_index(db, index_name):
    try:

        db.eye_01_pages.create_index(
            [("keyword",  TEXT)],
            unique=True, name=index_name
        )
    except Exception as er:
        print(er)
    client.close()

# create_multi_unique_index('imdb_keyword')

# ignore duplicate
def insert_mongo_doc(db, collection_name, doc):
    # SAVE DATA TO MONGO DB
    collection = db[collection_name]
    collection.with_options(write_concern=WriteConcern(w=0)).insert_one(doc)
    print(collection_name + ' mongo insert successfully')

def insert_many_mongo_docs(db, collection_name, doc_list):
    # SAVE DATA TO MONGO DB
    collection = db[collection_name]
    collection.insert_many(doc_list)
    print(collection_name + ' mongo insert successfully')


def get_mongo_doc_by_date(db, start, end, collection_name, douban_id=None):
    # $gte: greater than or equal to start_date
    # $lte: less than or equal to end_date
    mongo_col = db[collection_name]
    find_condition = {'created_date': {'$gte': start, '$lte': end}}
    if douban_id != None:
        find_condition.update({'douban_id': douban_id})

    docs = mongo_col.find(find_condition)
    return docs

def get_mongo_doc_by_id(db, collection_name, id_type, id):
    # $gte: greater than or equal to start_date
    # $lte: less than or equal to end_date
    mongo_col = db[collection_name]
    find_condition = {id_type: id}
    docs = mongo_col.find(find_condition)

    return docs

def get_mongo_doc(db, collection_name):
    mongo_col = db[collection_name]
    docs = mongo_col.find({})
    return docs


