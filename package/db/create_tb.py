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

def create_table_error_log():
    connection = init_db()

    # Create table movie.main_info
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS error_log ("
                   "`file_name` varchar(255),"
                   "`func_name`varchar(255),"
                   "`log_time`datetime,"
                   "`imdb_id` varchar(255),"
                   "`spent` float,"
                   "`status_code` int,"
                   "`exc_type` varchar(255),"
                   "`line` varchar(255),"
                   "`msg` varchar(255))"
                   )
    connection.commit()

def create_table_data_processing_log():
    connection = init_db()

    # Create table movie.main_info
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS data_processing_log ("
                   "`file_name` varchar(255),"
                   "`func_name`varchar(255),"
                   "`log_time`datetime,"
                   "`imdb_id` varchar(255),"
                   "`spent` float,"
                   "`status_code` int)"
                   )
    connection.commit()



def create_table_processing_finished_log():
    connection = init_db()

    # Create table movie.main_info
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS processing_finished_log ("
                   "`file_name` varchar(255),"
                   "`func_name` varchar(255),"
                   "`start` datetime,"
                   "`end` datetime,"
                   "`spent` float,"
                   "`status_code` int,"
                   "`result_count` int)"
                   )
    connection.commit()

def create_table_douban():
    connection = init_db()

    # Create table movie.main_info
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS douban ("
                   "`douban_id`varchar(255),"
                   "`imdb_id` varchar(255),"
                   "`year` int)"
                   )
    connection.commit()

# create_table_douban()
def create_table_mapping():
    connection = init_db()
    # Create table movie.mapping
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS web_id_mapping ("
                   "`imdb_id` varchar(255),"
                   "`douban_id` int,"
                   "`tomoto` varchar(255),"
                   "`yahoo` int)"
                   )
    connection.commit()

def create_table_plot():
    connection = init_db()
    # Create table movie.mapping
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS plot ("
                   "`imdb_id` varchar(255),"
                   "`zh_plot` TEXT,"
                   "`en_plot` TEXT)"
                   )
    connection.commit()

def create_table_genre_type():
    connection = init_db()
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS genre_type ("
                  "`genre_id` int auto_increment PRIMARY KEY,"
                   "`genre` varchar(40) UNIQUE )"
                   )
    connection.commit()

def create_table_movie_genre():
    connection = init_db()
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS movie_genre ("
                   "`imdb_id` varchar(255),"
                    "`genre_id` int )"
                   )
    connection.commit()

# create_table_movie_genre()
def create_table_movie_info():
    connection = init_db()
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS movie_info ("
                  "`imdb_id` varchar(255) PRIMARY KEY,"
                   "`tw_name` varchar(255),"
                   "`en_name` varchar(255),"
                   "`runtime` int,"
                   "`published` char(12),"
                   "`genre` varchar(255),"
                   "`img` varchar(255))"
                   )
    connection.commit()

def create_table_other_names():
    connection = init_db()
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS other_names ("
                  "`imdb_id` varchar(255) PRIMARY KEY,"
                   "`lang_type` varchar(10),"
                   "`name` varchar(20))"
                   )
    connection.commit()

# create_table_other_names()

def create_table_staff():
    connection = init_db()
    # Create table movie.mapping
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS staff ("
                   "`imdb_movie` varchar(255),"
                   "`job_type` varchar(255),"
                   "`imdb_per` varchar(255),"
                   "`douban_per` varchar(255))"
                   )
    connection.commit()

def create_table_celebrity():
    connection = init_db()
    # Create table movie.mapping
    cursor = get_cursor(connection)
    cursor.execute("CREATE TABLE IF NOT EXISTS celebrity ("
                   "`imdb_per` varchar(255),"
                   "`en_name` varchar(255),"
                   "`douban_per` varchar(255),"
                   "`zh_name` varchar(255))"
                   )
    connection.commit()

