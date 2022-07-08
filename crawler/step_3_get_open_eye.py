from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import mysql.connector
from db_setting import connect_set
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime
from pymongo import MongoClient
from fake_useragent import UserAgent
import time
import requests
import re
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
import boto3
from random import randint
from package.db import s3

config = connect_set.rds.set
mongo = connect_set.mongo.set

mongo_pwd = mongo['password']
mongo_connection = "mongodb+srv://fuzzy_user:{pwd}@cluster0.n9qoj.mongodb.net/?retryWrites=true&w=majority".format(pwd=mongo_pwd)
client = MongoClient(mongo_connection)
db = client.movie
mongo_col = db.fuzzy_input_substr


config = connect_set.rds.set

def init_db():

    return mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database='movie')

def connect_s3():
    s3 = boto3.resource("s3")

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


log_path = 'get_eye_data.log'



# input: http://www.atmovies.com.tw/movie/fbfr13553662/
# output: fbfr13553662
def eye_id_filter(url):
    home = 'http://www.atmovies.com.tw/movie/'
    id = url.replace(home, '').rstrip('/')
    return id

# input: https://imdb.com/Title?13553662
# output: 13553662
def imdb_id_filter(url):
    home = 'https://imdb.com/Title?'
    id = 'tt' + url.replace(home, '').rstrip('/')
    return id

# '2022/05/25' -> 2022-05-25
def str_to_date(str):
    year = str.split('/')[0]
    month = str.split('/')[1]
    day = str.split('/')[2]
    merge = year + '-'+ month +'-' + day
    return merge


# input: 友你才精彩 BEAUTIFUL MINDS
# output: 友你才精彩 , BEAUTIFUL MINDS
def eye_title_split(str):
    str = str.rstrip(' ').lstrip(' ')
    result = re.match(r'[^a-zA-Z\d]{0,2}', str)

    # print('result', result)
    # zh_name + en_name
    if result.span() != (0, 0):
        title = str.split(' ',1)
        zh_title = title[0]
        en_title = title[1]
        return zh_title, en_title
    else:
        # only english name
        return None, str

# print(eye_title_split('Under the Boardwalk: The Monopoly Story') )

# input: text
# output: text
def clean_text(str):
    # remove title
    text_after = str.split('劇情簡介')[1]
    # removie a tag url
    remove_href = re.sub(r'href[^>]*', '', text_after)
    return remove_href




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

# input: eye_id
# output: zh movie data
def crawler_zh_eye_data(id):
    url = 'http://www.atmovies.com.tw/movie/' + id
    row = {}

    # Create driver
    user_agent = UserAgent()
    random_user_agent = user_agent.random

    eye_id = eye_id_filter(url)
    options = Options()
    options.add_argument(f'user-agent={random_user_agent}')
    # options.add_argument('--headless')
    # options.add_argument('--no-sandbox')
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    # Loading page
    try:
        driver.get(url)
        # Wait to load the page
        WebDriverWait(driver, 7).until(
            EC.presence_of_element_located((By.TAG_NAME, "footer"))
        )

    except:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write('func: eye_data\t' + id + '\tloading index page to scrape failed')
        return None

    # init value
    film_title = None
    zh_title = None
    en_title = None
    image_url = None

    film_title = driver.find_elements(By.CLASS_NAME, 'filmTitle')[0].text
    zh_title, en_title =eye_title_split(film_title)

    if driver.find_elements(By.CLASS_NAME, 'image.Poster'):
        image = driver.find_element(By.CLASS_NAME, 'image.Poster')
        if image.find_elements(By.TAG_NAME, 'img'):
            image_url = image.find_element(By.TAG_NAME, 'img').get_attribute('src')
            photo_body = requests.get(image_url).content
            photo_name = eye_id + '_main.jpg'

            s3.put_photo_to_s3(photo_name='testV4.jpg', photo_stream=photo_body)


    video = driver.find_elements(By.CLASS_NAME, 'image.featured')[0].get_attribute('src')

    time_div = driver.find_elements(By.CLASS_NAME, 'runtime')[0].find_elements(By.TAG_NAME, 'li')
    runtime = ''
    release_date = ''
    for li in time_div:
        # print(li.text)
        time_str = li.text
        if '片長' in time_str:
            runtime = time_str.replace('片長', '').lstrip('：').strip('分')
        elif '上映日期' in time_str:
            release_date = time_str.replace('上映日期', '').lstrip('：')  # 2022/06/18
            release_date = str_to_date(release_date)  # 2022-06-18

    # filmCastDataBlock
    stars = []
    zh_writer = None
    zh_director = None
    director_tuple = None
    writer_tuple = None

    if driver.find_elements(By.ID, 'filmCastDataBlock'):
        filmCastDataBlock = driver.find_element(By.ID, 'filmCastDataBlock')
        if filmCastDataBlock.find_elements(By.TAG_NAME, 'ul'):
          zh_ul = filmCastDataBlock.find_elements(By.TAG_NAME, 'ul')[0]  # only first ul
          li_list = zh_ul.find_elements(By.TAG_NAME, 'li')
          if li_list != []:
              star_begin = None
              for li in li_list:
                  if li.find_elements(By.TAG_NAME, 'a'):
                      li_a = li.find_element(By.TAG_NAME, 'a')
                      li_id = li_a.get_attribute('href').lstrip('http://app2.atmovies.com.tw/star/').rstrip('/')
                      # print('li_a.text', li_a.text, 'li_id', li_id)
                      # http://www.atmovies.com.tw/movie/fbfr13553662/ -- > fbfr13553662
                      if '導演' in li.text:
                          zh_director = li_a.text
                          star_id = li_id
                          director_tuple = (star_id, zh_director)
                      elif '編劇' in li.text:
                            zh_writer = li_a.text
                            star_id = li_id
                            writer_tuple = (star_id, zh_writer)
                      elif '演員' in li.text:
                            star = li_a.text
                            star_id = li_id
                            star_tuple = (star_id, star)
                            stars.append(star_tuple)
                            star_begin = 1
                      elif star_begin == 1 and li.find_elements(By.TAG_NAME, 'b') != []:
                            star_begin = None
                      elif star_begin == 1 and li.find_elements(By.TAG_NAME, 'b') == []:
                          if li_a.text not in ['MORE', 'more', 'More']:
                              star = li_a.text
                              star_id = li_id
                              star_tuple = (star_id, star)
                              stars.append(star_tuple)

    # text = driver.find_elements(By.XPATH, "//div[@style='width:90%;font-size: 1.1em;']")[0].text.lstrip('劇情簡介\n')
    if driver.find_elements(By.XPATH, "//div[@style='width:90%;font-size: 1.1em;']"):
        text_html = driver.find_element(By.XPATH, "//div[@style='width:90%;font-size: 1.1em;']").get_attribute('innerHTML')
        text = clean_text(text_html)
        # print('text', text)

    if director_tuple != None:
        row.update({'director': director_tuple})
    if writer_tuple != None:
        row.update({'writer': writer_tuple})

    row.update({'eye_id': eye_id, 'zh_title': zh_title, 'en_title': en_title, 'runtime': runtime,
     'release_date': release_date, 'video': video, 'text': text, 'star_dict': stars, 'image_url':image_url})
    # print('row', row)

    # if imdb link exist, merge_id is imdb_id
    if driver.find_elements(By.XPATH, "//a[contains(text(), 'IMDb')]"):
        imdb_link = driver.find_elements(By.XPATH, "//a[contains(text(), 'IMDb')]")[0].get_attribute('href')
        imdb_id = imdb_id_filter(imdb_link)  # 10298810 without head tt

        # merge_id == imdb_id, inser merge_id
        row.update({'imdb_id':imdb_id, 'merge_id': imdb_id})
        # print('row', row)
        write_main_info(row)
    else:
        # if imdb link not exist, merge_id is eye_id
        now = datetime.now()
        row.update({'imdb_id': None, 'merge_id':eye_id})
        write_main_info(row)
    driver.close()
    print('--------------------')
    return row


# crawler_zh_eye_data('fltw61553951')
# for i in ['ffjp19849494', 'fmjp15482370', 'feen96710474', 'frkr53839701', 'fltw61553951', 'fffr14317880', 'fmen11389748', 'flfr14039392', 'fpde21440266', 'fmen13841850', 'fGcmb7070658', 'flen30119558', 'ffuk84123432', 'fcjp15434284']:
#     crawler_zh_eye_data(i)


# crawler_zh_eye_data('feen10095624')
# crawler_zh_eye_data('fjen54586766')
# no imdb link
# crawler_zh_eye_data('http://www.atmovies.com.tw/movie/fdjp97741333/')

# 1918、2001 not finished

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

# write_search_by_eye_result( ('Shut In', 'fEatm0884089', '大開眼戒', 'Eyes Wide Shut', 'tt0120663', '1999') )

# for search_by_eye
# input: http://app2.atmovies.com.tw/film/fken40478311/
# output: imdb_id
def get_imdb_id(link):
    # Create driver
    # Create driver
    user_agent = UserAgent()
    random_user_agent = user_agent.random
    options = Options()
    options.add_argument(f'user-agent={random_user_agent}')
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    driver.get(link)
    try:
        # Wait to load the page
        WebDriverWait(driver, 2).until(
            EC.presence_of_element_located((By.ID, "filmCastDataBlock"))
        )
    except:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write('func: search_by_eye\t' + link + '\t get imdb_id failed')
            return None
    # if imdb link exist
    if driver.find_elements(By.XPATH, "//a[contains(text(), 'IMDb')]"):
        imdb_link = driver.find_element(By.XPATH, "//a[contains(text(), 'IMDb')]").get_attribute('href')
        imdb_id = imdb_id_filter(imdb_link)
    else:
        imdb_id = None
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write('func: get_imdb_id\t' + 'url' + link + '\t no imdb_id in page\n')
    driver.close()
    return imdb_id


# input: keyword
# output: save (keyword, eye_id,   zh_name, en_name, imdb_id) to SQLDB
def search_by_eye(str):
    imdb_list = []
    # with open('proxy_list.txt', 'r') as file:
    #     for idx, line in enumerate(file):
    #         if idx == num:
    #             ip = line.rstrip('\n')
    #             print(num, ip)
    #             break

    home = 'http://www.atmovies.com.tw/home/'
    # Create driver
    user_agent = UserAgent()
    random_user_agent = user_agent.random

    options = Options()
    # new_ip = "--proxy-server=http://" + ip
    # options.add_argument(new_ip)
    options.add_argument(f'user-agent={random_user_agent}')
    options.add_argument("start-maximized")
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    # Loading page
    try:
        driver.get(home)
        # Wait to load the page
        # print('Loading EYE HOME Page')
        WebDriverWait(driver, 4).until(
            EC.presence_of_element_located((By.ID, "searchForm"))
        )
    except:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write('func: search_by_eye\t' + 'keyword ' + str + '\tloading page to scrape failed\n')
            driver.close()
        return imdb_list

    # input movie name
    keyword = str
    # print('search word', keyword)
    search_input = driver.find_element(By.ID, 'search-field')
    search_input.send_keys(keyword)

    # click search button
    for t in range(1, 2):
        try:
            # Wait to load the page
            WebDriverWait(driver, 7).until(
                EC.element_to_be_loacted((By.CLASS_NAME, 'search_submit'))
            )
            break
        except:
            if t == 5:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write('func: search_by_eye' + '\tloading button to click failed\n')
            else:
                pass

    sleep(randint(0, 3))
    print('wait end')
    sleep(3)
    button = driver.find_element(By.CLASS_NAME, 'search_submit').click()

    # search result list
    for t in range(1, 6):
        try:
            # Wait to load the page
            WebDriverWait(driver, 7).until(
                EC.presence_of_element_located((By.TAG_NAME, 'blockquote'))
            )
            break
        except:
            if t == 5:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write('func: search_by_eye\t' + keyword + '\tloading result page to scrape failed\n')
                    driver.close()
                return imdb_list
            else:
                pass
    sleep(2)
    movie_result_count = 0  # if search no result, value = 0

    imdb_id = None
    zh_name = None
    en_name = None
    year = None
    eye_id = None

    if driver.find_elements(By.TAG_NAME, 'blockquote'):
        blockquote = driver.find_element(By.TAG_NAME, 'blockquote')

        # many result
        if blockquote.find_elements(By.TAG_NAME, 'header'):
            li_list = blockquote.find_elements(By.TAG_NAME, 'header')
            for idx, li in enumerate(li_list):
                # print('header', li.text)
                sleep(randint(0, 2))

                if li.find_elements(By.CSS_SELECTOR, "font[color='#FF0000']"):
                    font_type = li.find_element(By.CSS_SELECTOR, "font[color='#FF0000']").text
                    if font_type == '電影':
                        movie_result_count = movie_result_count +1
                        movie_name = li.find_element(By.TAG_NAME, 'a').text
                        zh_name, en_name = eye_title_split(movie_name)
                        movie_url = li.find_element(By.TAG_NAME, 'a').get_attribute('href')
                        eye_id = movie_url.replace('http://search.atmovies.com.tw/F/', '').rstrip('/')
                        imdb_id = get_imdb_id(movie_url)
                        imdb_list.append(imdb_id)

                        # Year
                        if li.find_elements(By.CSS_SELECTOR, "font[color='#606060']"):
                            year = li.find_element(By.CSS_SELECTOR, "font[color='#606060']").text

                        # Inser keyword, eye_id,   zh_name, en_name, imdb_id into DB
                        tuple = (keyword, eye_id, zh_name, en_name, imdb_id, year)
                        print('Ready to write tuple')
                        print('tutple 1', tuple)
                        write_search_by_eye_result(tuple)
        # only one result
        else:
            if blockquote.find_elements(By.TAG_NAME, 'font'):
                if blockquote.find_element(By.TAG_NAME, 'font').text =='電影':
                    movie_result_count = movie_result_count + 1
                    title = blockquote.find_element(By.CLASS_NAME, 'title.big').text
                    zh_name, en_name = eye_title_split(title)
                    movie_url = blockquote.find_element(By.CLASS_NAME, 'title.big').get_attribute('href')
                    imdb_id = get_imdb_id(movie_url)
                    imdb_list.append(imdb_id)


                    eye_id = movie_url.replace('http://search.atmovies.com.tw/F/', '').rstrip('/')

                    if blockquote.find_elements(By.CSS_SELECTOR, "[color='#606060']"):
                        year = blockquote.find_element(By.CSS_SELECTOR, "[color='#606060']").text

                    # print(keyword, eye_id, zh_name, en_name, imdb_id, year)
                    tuple = (keyword, eye_id, zh_name, en_name, imdb_id, year)
                    print('tutple 2', tuple)
                    write_search_by_eye_result(tuple)
    driver.quit()

    if movie_result_count == 0:
        with open(log_path, 'a', encoding='utf-8') as f:
            now = datetime.now()
            current_time = now.strftime('%Y-%m-%d %H:%M:%S')
            f.write(current_time + '\tfunc: search_by_eye()\t' + 'search keyword\t'+ keyword + '\tsearch result None\n')
        print('search result None')


        return imdb_list
    else:
        print('search and save finished')
        return imdb_list


# search_by_eye('天天初體驗')

def all_zh_data_from_db():
    # create table
    connection = init_db()
    cursor = get_cursor(connection,opt=True)
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


# input: mongodb fuzzy data
# output: save to  movie.search_by_eye_result;
def serch_by_eye_from_mongodb():
    cursor = mongo_col.find({})
    collection_data = [document for document in cursor]
    for idx, document in enumerate(collection_data):

        # document : {'_id': ObjectId('62b6afca1e8047a974c48a8c'), 'imdb_id': 'tt0000001', 'type': 'Movie', 'title': 'Carmencita', 'og_title': None, 'year': '1894', 'fuzzy': ['Carmencita']}
        # print(document)
        fuzzy_strs = document['fuzzy']
        year = document['year']
        for str in fuzzy_strs:
            print('search times idx', idx, 'serch_by_eye_from_mongodb keyword', str)
            search_by_eye(str)
        print('-----------------------------')


# serch_by_eye_from_mongodb()





