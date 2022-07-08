from package.db.sql import init_db, get_cursor


def create_tb_imdb_id():
    # create table
    connection = init_db()
    cursor = get_cursor(connection)
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS movie.imdb_movie_id ("
        "`imdb_id` varchar(15) PRIMARY KEY,"
        "`type` varchar(20),"
        "`title` varchar(255),"
        "`og_title` varchar(255),"
        "`year` char(4))"
                   )
    connection.commit()

def create_tb_eye_01():
    # Create table rating
    connection = init_db()
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS eye_01 ("
                   "`id` int PRIMARY KEY,"
                   "`date` char(10),"
                  "`time` int,"
                   "`imdb_id` varchar(255),"
                   "`word` varchar(255),"
                   "`result_count` int,"
                   "UNIQUE KEY(imdb_id, word) )"
                   )
    connection.commit()




def create_tb_eye_02_id():
    # Create table rating
    connection = init_db()
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS eye_02_id ("
                   "`mongo_start` char(10),"
                   "`mongo_end` char(10),"
                   "`eye_id` varchar(255) PRIMARY KEY )"
                   )
    connection.commit()

create_tb_eye_02_id()


def create_tb_rating():
    # Create DB movie
    connection = init_db()
    cursor = get_cursor(connection)
    cursor.execute("CREATE DATABASE IF NOT EXISTS rating")
    connection.commit()

    # Create table rating
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS rating ("
                   "`merge_id` varchar(255) PRIMARY KEY,"
                  "`imdb_id` varchar(255) UNIQUE,"
                   "`imdb_score` float,"
                   "`imdb_count` int,"
                   "`tomato_meter` int,"
                   "`tomato_audience` int,"
                   "`tomato_review_count` int,"
                   "`tomato_audience_count` int,"
                   "`meta_score` int,"
                   "`meta_score_count` int,"
                   "`meta_user_score` int,"
                   "`meta_user_count` int,"
                   "`yahoo_score` float,"
                   "`yahoo_count` int)"
                   )
    connection.commit()
    return
# create_tb_rating()