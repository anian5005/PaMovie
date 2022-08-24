from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import mysql.connector
from db_setting import connect_set
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import json
from time import sleep
from fake_useragent import UserAgent
from package import nlp


config = connect_set.rds.set
log_path = '../trash/old_version/log/tomato.log'

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





def imdb_rating(imdb_id):
    rating = None

    url = 'https://www.imdb.com/title/'+ imdb_id

    # Create driver
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    try:
        driver.get(url)
        # Wait to load the page
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CLASS_NAME, "sc-94726ce4-0.cMYixt"))
        )
        driver.get(url)
        page = driver.page_source
    except:
        print('get page Error by selenium')

    imdb_rating = None
    imdb_count = None
    # application/ld+json
    if driver.find_elements(By.XPATH, "//script[@type='application/ld+json']"):
        script_str = driver.find_element(By.XPATH, "//script[@type='application/ld+json']").get_attribute('innerHTML')
        script_json = json.loads(script_str)
        if script_json.get('aggregateRating', None) != None:
            rating_dict = script_json['aggregateRating']
            print('rating_dict', rating_dict)
            imdb_count = str(rating_dict['ratingCount']).replace(',', '').rstrip('+')
            imdb_rating = rating_dict['ratingValue']

    print(imdb_id, imdb_id , imdb_rating, imdb_count)
    return (imdb_id, imdb_id , imdb_rating, imdb_count)


def write_imdb_rating(tuple):
    insert_tuple = tuple
    # (imdb_id, imdb_id , imdb_rating, imdb_count)

    connection = init_db()
    cursor = get_cursor(connection)
    sql = "INSERT IGNORE INTO rating (merge_id, imdb_id, imdb_score, imdb_count) VALUES (%s, %s, %s, %s)"
    cursor.execute(sql, tuple)
    connection.commit()

test2 = imdb_rating('tt0326716')
write_imdb_rating(test2)



def tomatometer(merge_id, url):
    # Create driver
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    # Loading page
    for times in range(1,6):

        try:
            driver.get(url)
            # Wait to load the page
            WebDriverWait(driver, 7).until(
                EC.presence_of_element_located((By.TAG_NAME, "footer"))
            )
            break

        except:
            if times == 5:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write('func: tomatometer\t' + id + '\tloading index page to scrape failed')
                return None
            else:
                pass

    audience_score = None
    tomato_meter_score = None
    tomato_review_count = None
    tomato_audience_count = None
    page = driver.page_source
    # print(page)



    try:
        score_details_json = driver.find_element(By.ID, 'score-details-json').get_attribute('innerHTML')
        json_data = json.loads(score_details_json)
        # print('json_data', json_data)
        tomatometerScore = json_data['scoreboard']['tomatometerScore']
        tomatometerCount = str(json_data['scoreboard']['tomatometerCount']).replace(',', '').rstrip('+')
        audienceScore = json_data['scoreboard']['audienceScore']
        audienceBandedRatingCount = str(json_data['scoreboard']['audienceBandedRatingCount']).replace(',', '').rstrip('+')

        print('tomatometerScore', tomatometerScore)
        print('tomatometerCount', tomatometerCount)
        print('audienceScore', audienceScore)
        print('audienceBandedRatingCount', audienceBandedRatingCount)

        print({'merge_id': merge_id, 'tomato_meter':tomatometerScore, 'tomato_audience':audienceScore, 'tomato_review_count': tomatometerCount, 'tomato_audience_count': audienceBandedRatingCount})
        return {'merge_id': merge_id, 'tomato_meter':tomatometerScore, 'tomato_audience':audienceScore, 'tomato_review_count': tomatometerCount, 'tomato_audience_count': audienceBandedRatingCount}

    except:
        print('error')
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write('func:  tomatometer\t' + 'merge_id\t' + merge_id + '\tscrape rating failed\n')

    # score_board = driver.find_element(By.CLASS_NAME, 'scoreboard')
    # # data-qa="tomatometer"
    # if score_board.find_elements(By.XPATH, "//score-board[@tomatometerstate='certified-fresh']"):
    #     score_board = score_board.find_element(By.XPATH, "//score-board[@tomatometerstate='certified-fresh']")
    #     tomato_meter_score = score_board.get_attribute('tomatometerscore')
    #     audience_score = score_board.get_attribute('audiencescore')
    #     # print('tomatometerscore', tomato_meter_score)
    #     # print('audiencescore', audiencescore)
    #
    #
    # if driver.find_elements(By.ID, 'score-details-json'):
    #     score_details_json = driver.find_element(By.ID, 'score-details-json')
    #     # print('score_details_json', score_details_json)
    # #     # print('review_count', review_count)
    #     str_script = score_details_json.get_attribute('innerHTML')
    #     # print('str_script', str_script)
    #     json_data = json.loads(str_script)
    #     # print('json_data', json_data)
    #     scoreboard = json_data.get('scoreboard', None)
    #     if scoreboard != None:
    #         # print('scoreboard', scoreboard)
    #         tomato_review_count = str(scoreboard['tomatometerCount']).replace(',', '').rstrip('+')
    #         tomato_review_count = int(tomato_review_count)
    #
    #         tomato_audience_count = str(scoreboard['audienceBandedRatingCount']).replace(',', '').rstrip('+')
    #         tomato_audience_count = int(tomato_audience_count)
    #         # print('tomato_review_count', tomato_review_count, 'tomato_audience_count', tomato_audience_count)
    #
    # print({'merge_id': merge_id, 'tomato_meter':tomato_meter_score, 'tomato_audience':audience_score, 'tomato_review_count': tomato_review_count, 'tomato_audience_count': tomato_audience_count})
    # return {'merge_id': merge_id, 'tomato_meter':tomato_meter_score, 'tomato_audience':audience_score, 'tomato_review_count': tomato_review_count, 'tomato_audience_count': tomato_audience_count}

# tomatometer('666', 'https://www.rottentomatoes.com/m/doctor_strange_in_the_multiverse_of_madness')


def write_totmoto_rating(rating_dict):
    connection = init_db()
    merge_id = rating_dict['merge_id']
    tomato_meter = rating_dict['tomato_meter']
    tomato_audience = rating_dict['tomato_audience']
    tomato_review_count = rating_dict['tomato_review_count']
    tomato_audience_count =rating_dict['tomato_audience_count']

    tuple = (tomato_meter, tomato_audience, tomato_review_count, tomato_audience_count, merge_id)

    # cursor = get_cursor(connection)
    # sql = "INSERT INTO rating (merge_id, tomato_meter, tomato_audience, tomato_review_count, tomato_audience_count) VALUES (%s, %s, %s, %s, %s)"
    # cursor.execute(sql, (merge_id, tomato_meter, tomato_audience, tomato_review_count, tomato_audience_count))
    # connection.commit()

    #{'merge_id': merge_id, 'tomato_meter':tomato_meter_score, 'tomato_audience':audience_score, 'tomato_review_count': tomato_review_count, 'tomato_audience_count': tomato_audience_count}
    # try update exist row
    try:

        cursor = get_cursor(connection)
        sql = "UPDATE rating SET tomato_meter=%s, tomato_audience=%s, tomato_review_count=%s, tomato_audience_count=%s WHERE merge_id = %s"
        # cursor.execute("UPDATE table_name SET field1=%s, ..., field10=%s WHERE id=%s", (var1,... var10, id))
        cursor.execute(sql, tuple)
        connection.commit()
    except:
        cursor = get_cursor(connection)
        sql = "INSERT INTO rating (merge_id, tomato_meter, tomato_audience, tomato_review_count, tomato_audience_count) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(sql, (merge_id, tomato_meter, tomato_audience, tomato_review_count, tomato_audience_count) )
        connection.commit()
    finally:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write('func: write_totmoto_rating\t' + str(rating_dict) + '\tfailed')


# test2 = imdb_rating('tt1745960')
# write_imdb_rating(test2)

# url = 'https://www.rottentomatoes.com/m/top_gun_maverick'
# test = tomatometer('tt1745960', url)
# t = {'merge_id': '111', 'tomato_meter': 74, 'tomato_audience': 85, 'tomato_review_count': '430', 'tomato_audience_count': '10000'}
# write_totmoto_rating(t)

def yahoo_rating(url):
    # Create driver
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    try:
        driver.get(url)
        # Wait to load the page
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.ID, "kv"))
        )
        page = driver.page_source

        # print('page', page)
    except:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write('func: yahoo_rating\t' + id + '\tloading index page to scrape failed')
        return None
    rating_value = None
    rating_count = None

    if driver.find_elements(By.XPATH, "//script[@type='application/ld+json']"):
        script_str = driver.find_element(By.XPATH, "//script[@type='application/ld+json']").get_attribute('innerHTML')
        script_json = json.loads(script_str)
        # print('script_json', script_json)
        if script_json.get('aggregateRating', None) != None:
            aggregate_rating = script_json['aggregateRating']
            # print('aggregate_rating', aggregate_rating)
            if aggregate_rating.get('ratingCount', None) != None:
                rating_count = aggregate_rating['ratingCount']
                # print('rating_count', rating_count)
            if aggregate_rating.get('ratingValue', None) != None:
                rating_value = aggregate_rating['ratingValue']
                # print('rating_value', rating_value)

    dict = {'rating_value': rating_value, 'rating_count': rating_count}
    return dict

# yahoo_url = 'https://movies.yahoo.com.tw/movieinfo_main/%E6%8D%8D%E8%A1%9B%E6%88%B0%E5%A3%AB-%E7%8D%A8%E8%A1%8C%E4%BF%A0-font-classhighlighttop-gun-maverickfont-10159'
# yahoo_rating(yahoo_url)



# Search movie by tomoto, then get rating to save db
def tomoto_rating_process(merge_id, target_movie, target_year):
    home = 'https://www.rottentomatoes.com/'

    # Create driver
    user_agent = UserAgent()
    random_user_agent = user_agent.random
    # print('random_user_agent', random_user_agent)

    options = Options()
    options.add_argument(f'user-agent={random_user_agent}')
    # options.add_argument('--headless')
    # options.add_argument('--no-sandbox')
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    # Loading home page, try 5 times
    for times in range(1, 6):
        try:
            # Wait to load the page
            driver.get(home)
            WebDriverWait(driver, 7).until(
                EC.presence_of_element_located((By.CLASS_NAME, "masthead__navbar"))
            )
            break
        except:
            if times == 5:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write('func: tomoto_search\t' + target_movie + '\tloading index page to scrape failed\n')
            else:
                pass

    if driver.find_elements(By.XPATH, "//input[@class='search-text']"):
        print('11111111')
        input_div = driver.find_element(By.XPATH, "//input[@class='search-text']")
        button = driver.find_element(By.XPATH, "//a[@class='search-submit']")
        # input keyword
        input_div.send_keys(target_movie)
        button.click()

        # Loading search result page, try 5 times
        for times in range(1, 6):
            print('222222222 times', times)
            try:
                # Wait to load the page
                WebDriverWait(driver, 7).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "footer__content-mobile-block"))
                )
                break
            except:
                if times == 5:
                    with open(log_path, 'a', encoding='utf-8') as f:
                        f.write('func: tomoto_search\t' + target_movie + '\tloading result page to scrape failed\n')
                    return None
                else:
                    pass

        # show search result
        page = driver.page_source
        # print('page', page)
        if driver.find_elements(By.CSS_SELECTOR, "search-page-result[slot='movie']"):
            print('333333333')
            movie_div = driver.find_element(By.CSS_SELECTOR, "search-page-result[slot='movie']")
            # print('movie_div', movie_div.get_attribute('innerHTML'))

            if movie_div.find_elements(By.CSS_SELECTOR, "search-page-media-row[data-qa='data-row']"):
                row_list = movie_div.find_elements(By.CSS_SELECTOR, "search-page-media-row[data-qa='data-row']")
                # print('row_list', row_list)
                for row in row_list:
                    # print(row.get_attribute('innerHTML'))
                    year = row.get_attribute('releaseyear')
                    # print('year', year)
                    name = row.find_element(By.CSS_SELECTOR, "[data-qa='info-name']").text
                    # counting
                    print('target_movie', target_movie, 'name', name)
                    ratio = nlp.match_preprocessing(target_movie, name)
                    print('ratio', ratio)
                    print('target_year == year',target_year, year)
                    if target_year != '' and year != '':
                        if ratio > 0.8 and abs(int(target_year) - int(year)) < 2:
                            print('match')
                            url = row.find_element(By.CSS_SELECTOR, "[data-qa='info-name']").get_attribute('href')
                            # print('url', url)
                            # Scrape tomoto rating
                            dict = tomatometer(merge_id, url)
                            print('dict', dict)
                            # {'merge_id': merge_id, 'tomato_meter':tomato_meter_score, 'tomato_audience':audience_score, 'tomato_review_count': tomato_review_count, 'tomato_audience_count': tomato_audience_count}
                            write_totmoto_rating(dict)
                        else:
                            print('no match')
                    print('************')
    driver.close()

# tomoto_rating_process('tt1745960', 'Top Gun: Maverick', '2022')
# tomoto_rating_process('5656', 'Doctor Strange in the Multiverse of Madness', '2022')








def yahoo_rating_process(merge_id, target_movie, target_year):
    search_api = 'https://movies.yahoo.com.tw/moviesearch_result.html?movie_type=movie&keyword='
    url = search_api + target_movie

    # Create driver
    user_agent = UserAgent()
    random_user_agent = user_agent.random

    options = Options()
    options.add_argument(f'user-agent={random_user_agent}')
    # options.add_argument('--headless')
    # options.add_argument('--no-sandbox')
    # options.add_argument("--window-size=1920,1080")

    # create driver
    for i in range(1, 6):
        try:
            driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
            break
        except:
            if i == 5:
                return
            else:
                pass
    # Loading home page, try 5 times
    for times in range(1, 6):
        try:
            # Wait to load the page
            driver.get(url)
            WebDriverWait(driver, 7).until(
                EC.presence_of_element_located((By.CLASS_NAME, "btn_plus_more"))
            )
            break
        except:
            if times == 5:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write('func: 0000000000000\t' + target_movie + '\t000000000000000000\n')
                    driver.quit()
            else:
                pass
    sleep(8)
    page = driver.page_source
    soup = BeautifulSoup(page, 'lxml')

    # print(page)
    try:
        searchpage = soup.findAll('div', {'class': 'searchpage'})

        try:
            # if result != None
            for type_div in searchpage:
                # print('type_div', type_div)
                # print(type_div.get_attribute('innerHTML'))
                search_num_div = type_div.find('div', {'class': 'search_num _c'})
                result_type = search_num_div.text
                if '作品搜尋結果' in result_type:
                    # print('result_type', result_type)
                    release_list = type_div.find('ul', {'class': 'release_list mlist'})
                    li_list = release_list.findALL('li')
                    for card in li_list:
                        print('card', card)
                        searchpage_info = card.find('div', {'class': 'searchpage_info'})
                        release_movie_name = searchpage_info.find('div', {'class': 'release_movie_name'})
                        zh_name = release_movie_name.find('a').text
                        en_name = release_movie_name.find('div', {'class': 'en'}).text
                        release_date = release_movie_name.find('div', {'class': 'time'}).text.replace('上映日期', '').replace(':', '').strip()  # 2021-01-15
                        year = release_date.split('-')[0]
                        print('zh_name', zh_name, 'en_name', en_name)
                        print('release_date', release_date)

                        # count movie name ratio
                        try:
                            if target_year != '' and year != '':
                                ratio = nlp.match_preprocessing(target_movie, en_name)
                                print('ratio', ratio)
                                if ratio > 0.8 and abs(int(target_year) - int(year)) < 2:

                                    print('HIT！！！', 'zh_name', zh_name, 'en_name', en_name)

                        except Exception as er:
                            print('can not matcher by year or name', er)


                        print('-----------------------------')

        except Exception as er:
            driver.quit()
            print(er)
            print('no movie result' + '\t' + er)

    except Exception as er:
        driver.quit()
        print(er)
        print('no search result'  + '\t' + er)




# yahoo_rating_process('123', 'up', 2022)





def test():
    connection = init_db()
    cursor = get_cursor(connection, opt=False)
    sql = "SELECT merge_id, en_title, release_date FROM movie.main_info"
    cursor.execute(sql)
    out = cursor.fetchall()

    for idx, row in enumerate(out):
        if idx >= 23:
            merge_id = row[0]
            en_title = row[1]
            if row[2] != None:
                year = row[2].split('-')[0]
                print('idx', idx, 'merge_id：', merge_id, 'en_title：', en_title, 'year：', year)
                tomoto_rating_process(merge_id, en_title, year)

                # yahoo_rating_process(merge_id, en_title, year)
            else:
                print('failed id', merge_id)

                print('---------------------------')

    # yahoo_rating_process(merge_id, target_movie, target_year)
    # for imdb_id in out:
    #     tuple = imdb_rating(imdb_id)
    #     write_imdb_rating(tuple)

    # print('out', out)



# test()