import json, re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import mysql.connector
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import datetime
from db_setting import connect_set

config = connect_set.rds.set

def init_db():

    return mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database='movie')

def get_cursor(connection):
    try:
        connection.ping(reconnect=True, attempts=3, delay=2)
    except mysql.connector.Error as err:
        # reconnect your cursor as you did in __init__ or wherever
        connection = init_db()
    return connection.cursor(buffered=True)

def write_imdb_movie_id(tuple):
    # create table
    connection = init_db()
    # (zero_filled_number, title, original_title_name, year)
    cursor = get_cursor(connection)
    row = tuple
    sql = "INSERT IGNORE INTO imdb_movie_id (imdb_id, type, title, og_title, year) VALUES (%s,%s,%s,%s,%s)"
    cursor.execute(sql, row)
    connection.commit()
    return


def movie_simple_info(id):
    # set search language in header
    language_options = {'us': 'en-us,en;q=0.8,de-de;q=0.5,de;q=0.3', 'zh': 'zh-TW,zh;q=0.9'}
    language_set = language_options['zh']
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument("--start-maximized")
    options.add_experimental_option('prefs', {'intl.accept_languages': language_set})
    options.add_argument('window-size=1440x900')
    driver = webdriver.Chrome(options=options)

    url = 'https://www.imdb.com/title/' + id

    # Loading page
    try:
        driver.get(url)
        # Wait to load the page
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CLASS_NAME, "ipc-title__text"))
        )
        # print('html', html)
    except:
        # if page not exist
        log_time = time.time()
        with open(log_path, 'a', encoding='utf-8') as f:
            current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            f.write(current_time +'\tmovie_simple_info()\t' + 'id\t' + id + '\tloading page to scrape failed' + '\tlink\t' + url + '\n')
        pass

    # inti tuple value
    title = None
    original_title_name = None
    year = None
    id_type = None

    # {"@context":"https://schema.org","@type":"Movie"..
    # find json data in tag head
    if driver.find_element(By.TAG_NAME, 'head').find_elements(By.XPATH, "//script[@type='application/ld+json']"):
        head_str = driver.find_element(By.TAG_NAME, 'head').find_element(By.XPATH, "//script[@type='application/ld+json']").get_attribute('innerHTML')
        # print(head_str)
        head_json = json.loads(head_str)
        # print(head_json)
        # find id data type, movie or game ...
        if head_json.get('@type', None) != None:
            id_type = head_json['@type']
            # print('id_type', id_type)

            if id_type.lower() == 'movie':

                if driver.find_elements(By.XPATH, "//h1[@data-testid='hero-title-block__title']"):
                    title = driver.find_element(By.XPATH, "//h1[@data-testid='hero-title-block__title']").text
                    # print('title', title)

                if driver.find_elements(By.XPATH, "//div[@data-testid='hero-title-block__original-title']"):
                    original_title_div = driver.find_element(By.XPATH, "//div[@data-testid='hero-title-block__original-title']").text.lower()
                    remove = 'Original title'.lower()
                    original_title_name = original_title_div.replace(remove, '').replace(':', '').lstrip().rstrip()
                    # print('original_title_name', original_title_name)

                if driver.find_elements(By.CLASS_NAME, 'ipc-link.ipc-link--baseAlt.ipc-link--inherit-color.sc-8c396aa2-1.WIUyh'):
                    yr_test = driver.find_element(By.CLASS_NAME, 'ipc-link.ipc-link--baseAlt.ipc-link--inherit-color.sc-8c396aa2-1.WIUyh').text

                    year_pattern = r'\d{0,4}'
                    result = re.search(year_pattern, yr_test).group()
                    if len(result) == 4:
                        year = result
            driver.close()
            return (id, id_type, title, original_title_name, year)

    else:
        driver.close()
        return (id, id_type, title, original_title_name, year)




# print(movie_simple_info('tt11128440'))


# test = 'https://www.imdb.com/search/title/?title_type=feature,tv_movie,tv_special,documentary,short,video'
test2 = 'https://www.imdb.com/search/title/?title_type=feature,tv_movie,tv_special,documentary,short,video&after=WzM4MDM0LCJ0dDUzMDQ5OTYiLDI3NTAxXQ%3D%3D&ref_=adv_nxt'

#45329 6/29
#47814 6/29
#48452 6/30
log_path = 'log/crawler.log'

def imdb_id_V2(url, count):
    # set search language in header
    language_options = {'us': 'en-us,en;q=0.8,de-de;q=0.5,de;q=0.3', 'zh': 'zh-TW,zh;q=0.9'}
    language_set = language_options['zh']
    options = webdriver.ChromeOptions()
    options.add_argument("start-maximized")
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')

    options.add_experimental_option('prefs', {'intl.accept_languages': language_set})

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    now = datetime.now()

    try:
        driver.get(url)
        # Wait to load the page
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CLASS_NAME, "article"))
        )
    except:
        # if page not exist
        with open(log_path, 'a', encoding='utf-8') as f:
            current_time = now.strftime('%Y-%m-%d %H:%M:%S')
            f.write(current_time + '\timdb_id_V2()\t' + '\tloading page to scrape failed' + '\tlink\t' + url + '\n')
        pass

    if driver.find_elements(By.CLASS_NAME, 'article'):
        article_div = driver.find_element(By.CLASS_NAME, 'article')
        movie_lists = article_div.find_elements(By.CLASS_NAME, 'lister-item.mode-advanced')

        for movie_div in movie_lists:
            if movie_div.find_elements(By.CLASS_NAME, 'lister-item-header'):
                item_header = movie_div.find_element(By.CLASS_NAME, 'lister-item-header')
                if item_header.find_elements(By.TAG_NAME, 'a'):
                    header_a = item_header.find_element(By.TAG_NAME, 'a')
                    movie_url = header_a.get_attribute('href')
                    # print('movie_url', movie_url)
                    imdb_id = re.search(r'tt\d+', movie_url).group()
                    # print('imdb_id', imdb_id)
                    # (id, id_type, title, original_title_name, year)
                    tuple = movie_simple_info(imdb_id)
                    print(tuple)
                    # print('tuple', tuple)
                    try:
                        write_imdb_movie_id(tuple)
                        # print('------------------------')
                    except:
                        with open(log_path, 'a', encoding='utf-8') as f:
                            current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                            f.write(current_time + '\tfunc: get_imdb_movie_id\t' + 'imdb_id\t' + imdb_id + '\twrite to movie.imdb_movie_id failed\n')
                else:
                    with open(log_path, 'a', encoding='utf-8') as f:
                        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                        f.write(current_time + '\tfunc: imdb_id\t' + 'get imdb_id, title, og_title, year failed \n')

    else:
        with open(log_path, 'a', encoding='utf-8') as f:
            now = datetime.now()
            current_time = now.strftime('%Y-%m-%d %H:%M:%S')
            f.write(current_time + '\timdb_id_V2()\t' + '\tlocated class_name article failed' + '\tlink\t' + url + '\n')
            count = count + 1

    # check next page
    if driver.find_elements(By.XPATH, "//a[@class='lister-page-next next-page']"):
        next_link = driver.find_element(By.XPATH, "//a[@class='lister-page-next next-page']").get_attribute('href')
        print('next_link', next_link)
        driver.close()
        return imdb_id_V2(next_link, count)
    else:
        with open(log_path, 'a', encoding='utf-8') as f:
            now = datetime.now()
            current_time = now.strftime('%Y-%m-%d %H:%M:%S')
            f.write(current_time +'\tfunc: imdb_id_V2()\t' + 'finish all page and get\t' + str(count) + '\tmovie data' +'\n')
        return


# count =0
# imdb_id_V2(test2, count)

