from pymongo.write_concern import WriteConcern
from pymongo import MongoClient
from db_setting import connect_set

mongo = connect_set.mongo.set

mongo_pwd = mongo['password']
aws = mongo['aws']


def create_mongo_connection():
    mongo_conn_str = 'mongodb://local:{pwd}@{aws}:27017/movie'.format(pwd=mongo_pwd, aws=aws)
    mongo_connection = MongoClient(mongo_conn_str)
    mongo_db = mongo_connection.movie
    return mongo_db, mongo_connection


# ignore duplicate
def insert_mongo_doc(db, collection_name, doc):
    # SAVE DATA TO MONGO DB
    collection = db[collection_name]
    collection.with_options(write_concern=WriteConcern(w=0)).insert_one(doc)


def insert_many_mongo_docs(db, collection_name, doc_list):
    # SAVE DATA TO MONGO DB
    collection = db[collection_name]
    collection.insert_many(doc_list)


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


