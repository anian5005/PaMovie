from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import datetime
import requests

from package.db.sql import mark_eye_01, init_db, get_cursor, write_eye_01_log
from package.general import log_time
from package.db import mongo
from package.crawler import fuzzy_name


log_path = 'log/eye_01_test.log'


def eye_01_fuzzy_names(row):
    title = row['title']
    og_title = row['og_title']

    # Merge all zh_title or en_title to fuzzy
    if og_title != None:
        title_list = [title, og_title]
    else:
        title_list = [title]
    # Make each movie's fuzzy title list
    all_fuzzy_list = []
    for title in title_list:
        fuzzy_list = fuzzy_name(title)
        all_fuzzy_list = all_fuzzy_list + fuzzy_list
    return all_fuzzy_list

def eye_01_log(start, status, imdb_id, word, msg=None, result_count=None):
    # convert str to int
    if result_count != None:
        int_result_count = int(result_count)
    else:
        int_result_count = None

    current_time = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    end = time.time()
    spent = str(round(end - start, 2))

    log = {'current_time': current_time,
           'imdb_id': imdb_id,
           'word': word,
           'time': spent,
           'status': status,
           'msg': msg,
           'result_count': int_result_count}
    return log


# get eye search pages with keywords, save pages into mongo movie.eye_1_search_result
# 0: no result, status code: 1:o k,  2: save serch log to tb eye_01 error
# 3: save sql without mongo (save in eye_01)
def eye_01(imdb_id, word):
    imdb_id = str(imdb_id)
    search_word = str(word)
    func_name = 'eye_01'

    start = time.time()
    id = datetime.now().strftime('%m%d%H%M%S')  # 0704224730
    # print('id', id, 'date', date)

    home = 'http://www.atmovies.com.tw/home/'
    # Create driver
    user_agent = UserAgent()
    random_user_agent = user_agent.random

    options = Options()
    options.add_argument(f'user-agent={random_user_agent}')
    options.add_argument("start-maximized")
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')

    # create driver
    for i in range(1, 6):
        try:
            driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
            break
        except Exception as er:
            if i == 5:
                # mark_eye_01
                driver.quit()
                log = eye_01_log(start, 2, imdb_id, word, msg=er)
                return log
            else:
                pass

    # driver the page
    for i in range(1, 6):
        try:
            driver.get(home)
            break
        except Exception as er:
            if i == 5:
                # mark_eye_01
                driver.quit()
                log = eye_01_log(start, 2, imdb_id, word, msg=er)
                return log
            else:
                pass

    # Wait to load the page
    for i in range(1, 6):
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "searchForm"))
            )
            break

        except requests.exceptions.RequestException as e:
            if i == 5:
                    msg = 'loading searchForm to scrape failed: ' + e
                    driver.quit()
                    # mark_eye_01
                    log = eye_01_log(start, 2, imdb_id, word, msg=msg)
                    return log
            else:
                driver.quit()
                pass

        except Exception as er:
            if i == 5:
                msg = 'loading searchForm to scrape failed: ' + er
                driver.quit()
                # mark_eye_01
                log = eye_01_log(start, 2, imdb_id, word, msg=msg)
                return log
            else:
                driver.quit()
                pass

    # wait until search field present

    for t in range(1, 6):
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'search-field'))
            )
            break
        except Exception as er:
            if t == 5:
                msg = 'loading input field failed: ' + str(er)
                # mark_eye_01
                log = eye_01_log(start, 2, imdb_id, word, msg=msg)
                driver.quit()
                return log
            else:
                pass

    # input movie name
    for i in range(1, 6):
        try:
            print(func_name, 'search word', search_word)
            search_input = driver.find_element(By.ID, 'search-field')
            search_input.send_keys(search_word)
            search_input.submit()
            break
        except Exception as er:
            if i == 5:
                msg = 'search_input.submit failed: ' + str(er)
                # mark_eye_01
                log = eye_01_log(start, 2, imdb_id, word, msg=msg)
                driver.quit()
                return log
            else:
                pass

    # total result count
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, 'copyright'))
    )
    try:
        search_result_count = driver.find_element(By.CSS_SELECTOR, 'h2 > font').text.lstrip('(').rstrip(')')
        print(imdb_id, word, 'search_count', search_result_count)

        if int(search_result_count) == 0:
            # print('no result and save db log')
            try:
                # update scrape result into sql imdb_id
                # mark_eye_01
                driver.quit()
                log = eye_01_log(start, 0, imdb_id, word, msg=None, result_count=search_result_count)

                return log

            except Exception as er:
                msg = 'no result and save sql db log failed: ' + str(er)
                # mark_eye_01
                driver.quit()
                log = eye_01_log(start, 2, imdb_id, word, msg=msg, result_count=search_result_count)
                return log

        elif int(search_result_count) > 0:
            try:
                # save the page to  mongodb
                html_page = driver.page_source
                date_ymd = datetime.now().strftime('%Y-%m-%d')
                doc = {"created_date": date_ymd, 'id': id, 'imdb_id': imdb_id, 'search_word': search_word, 'page': html_page}
                mongo.insert_eye_search_result(doc)
                # update scrape result into sql imdb_id
                # mark_eye_01
                log = eye_01_log(start, 1, imdb_id, word, msg=None, result_count=search_result_count)
                driver.quit()
                return log

            except Exception as er:
                msg = 'mongo eye_01_pages insert failed: ' + str(er)
                # mark_eye_01
                log = eye_01_log(start, 3, imdb_id, word, msg=msg, result_count=search_result_count)
                driver.quit()
                return log

    except Exception as er:
        driver.quit()
        current_time = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        # empty page, not zero (word: Champagne!)
        # mark_eye_01
        print('Unknown error', str(er))
        msg = current_time + '\t' + imdb_id + '\t' + word + 'Unknown error: ' + str(er)
        log = eye_01_log(start, 2, imdb_id, word, msg=msg)
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(msg)
        return log

    driver.quit()
    return None


def count_sql_imdb_movie_id(status_list):
    if 2 in status_list or 3 in status_list:
        # any search = 2 or 3, output 2 (error status)
        return 2
    else:
        merge_status = 0
        for sub_status in status_list:
            merge_status = merge_status + int(sub_status)

        if merge_status == 0:
            return 0
        if merge_status > 0:
            return 1

def count_result_num(search_count_list):
    total = 0
    for num in search_count_list:
        if isinstance(num, int):
            total = total + num
    return total



# output: log_list
def eye_01_scrape_by_id(idx, imdb_id_dict):
    start = time.time()

    imdb_id = imdb_id_dict['imdb_id']
    all_fuzzy_list = eye_01_fuzzy_names(imdb_id_dict)

    current_time = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))  # 2022-07-17 11:44:45

    log_list = []
    sub_status_list = []
    result_count_list = []
    search_times_count = str(len(all_fuzzy_list))

    # collect log data
    for f_str in all_fuzzy_list:
        log = eye_01(imdb_id, f_str)
        sub_status = log['status']
        result_count = log['result_count']
        log_list.append(log)
        sub_status_list.append(sub_status)
        result_count_list.append(result_count)

    # mark sql eye_01 result
    status_code = count_sql_imdb_movie_id(sub_status_list)
    mark_eye_01(status_code, imdb_id)

    # count sql tb imdb_movie_id status
    total_result_count = count_result_num(result_count_list)

    # save log into sql  movie.eye_01_log
    # convert dict to tuple
    tuple_list = []
    for log_dict in log_list:
        # dict input: current_time, imdb_id, word, time, status, msg, result_count
        # tuple output: log_time, imdb_id, word, time, status, result_count, msg
        log_time = log_dict['current_time']
        imdb_id = log_dict['imdb_id']
        word = log_dict['word']
        spent_time = log_dict['time']
        status = log_dict['status']
        msg = log_dict['msg']
        result_count = log_dict['result_count']
        log_tuple = (log_time, imdb_id, word, spent_time, status, result_count, msg)
        tuple_list.append(log_tuple)

    write_eye_01_log(tuple_list)


    # save log into file
    end = time.time()
    spent = str(round(end - start, 2))
    msg = current_time + '\t' + 'imdb_id' + '\t' + imdb_id + '\t' + 'idx' + '\t' + str(idx) + '\t' + 'search_times' + '\t' + search_times_count + '\t' + 'total_result_count' + '\t' + str(total_result_count) +'\t' + 'spent' + '\t' + spent + '\n'
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(msg)



# output: imdb_id list
def eye_01_process(year):
    func_name = 'get_eye_01_fuzzy_name'
    start = time.time()

    # create sql conx
    while count < 5:
        count = count + 1
        try:
            connection = init_db()
            cursor = get_cursor(connection, opt=True)

        except Exception as e:
            if count == 4:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write(log_time() + '\t' + 'func: ' + func_name + '\t  Error Mesaage\t' + e + '\n')
                    return
            else:
                pass

    # get id list
    sql = "SELECT * FROM movie.imdb_movie_id  WHERE type ='Movie' and year = %s and eye_01 <=> NULL;"
    cursor.execute(sql, (year,))
    connection.commit()
    movie_ids = cursor.fetchall()

    # each imdb movie

    for idx, imdb_dict in movie_ids:
        eye_01_scrape_by_id(idx, imdb_dict)

    end = time.time()
    spent = end - start

    with open(log_path, 'a', encoding='utf-8') as f:
        msg = 'Total scrape： ' + 'eye_01 year' + year + ' spent time ' + str(spent) + '\n'
        f.write(msg)







