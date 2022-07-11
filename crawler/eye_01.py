from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
from random import randint
import time
from package.db.sql import write_eye_01,mark_eye_01
from package.general import log_time
from package.db import mongo
from datetime import datetime
import requests




# get eye search pages with keywords
# save pages into mongo movie.eye_1_search_result

# 0: no result, status code: 1:ok,  2: save serch log to tb eye_01 error
# 3: save sql without mongo (save in eye_01)
def eye_01(imdb_id, word):
    imdb_id = str(imdb_id)
    search_word = str(word)
    func_name = 'eye_01'
    log_path = 'crawler/log/eye_data.log'

    start = time.time()
    id = datetime.now().strftime('%m%d%H%M%S')  # 0704224730
    date = datetime.now().strftime('%Y-%m-%d')  # 2022-07-04
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
        except:
            if i == 5:
                mark_eye_01(2, imdb_id)
                driver.quit()
                return
            else:
                pass

    # driver the page
    for i in range(1, 6):
        try:
            driver.get(home)
            break
        except:
            if i == 5:
                mark_eye_01(2, imdb_id)
                driver.quit()
                return
            else:
                sleep(3)
                pass

    # Wait to load the page
    for i in range(1, 6):
        try:
            print('try')
            # print('Loading EYE HOME Page')
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "searchForm"))
            )
            break

        except requests.exceptions.RequestException as e:
            print('except')
            if i == 5:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write(log_time() + '\t' + 'func: ' + func_name + '\t' + 'search_word' + search_word + '\tloading searchForm to scrape failed\t' + e +'\n')
                    driver.quit()
                    mark_eye_01(2, imdb_id)
                    return
            else:
                driver.quit()
                sleep(10)
                print('try failed, sleep 5s')
                pass

        except Exception as er:
            print('except 2')
            print('Error Message', er)
            if i == 5:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write(log_time() + '\t' + 'func: ' + func_name + '\t' + 'search_word' + search_word+ '\tloading searchForm to scrape failed\t' + er +'\n')
                    driver.quit()
                    mark_eye_01(2, imdb_id)
                    return
            else:
                driver.quit()
                sleep(10)
                pass

    # wait until search field present

    for t in range(1, 5):
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'search-field'))
            )
            break
        except Exception as er:
            if t == 5:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write(log_time() + '\t' + 'func: ' + func_name + '\tsearch\t' + search_word + '\tloading input field failed\t' + er + '\n')
                    mark_eye_01(2, imdb_id)
                    return
            else:
                pass

    # input movie name
    for i in range(1, 6):
        try:
            print(func_name, 'search word', search_word)
            search_input = driver.find_element(By.ID, 'search-field')
            search_input.send_keys(search_word)
            search_input.submit()
            # sleep(randint(3, 7))
            break
        except:
            if i == 5:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write(log_time() + '\t' + 'func: ' + func_name + '\tsearch\t' + search_word + '\tsearch_input.submit failed\t' + er + '\n')
                    mark_eye_01(2, imdb_id)
                    driver.quit()
                    return
            else:
                pass

    print(func_name, ' wait end')


    # total result count
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, 'copyright'))
    )
    try:
        search_result_count = driver.find_element(By.CSS_SELECTOR, 'h2 > font').text.lstrip('(').rstrip(')')
        print('search_count', search_result_count)

        if int(search_result_count) == 0:
            # print('no result and save db log')
            end = time.time()
            spent_time = int(end - start)
            row = (id, date, spent_time, imdb_id, search_word, search_result_count)

            try:
                write_eye_01(row)
                # update scrape result into sql imdb_id
                mark_eye_01(0, imdb_id)
                driver.quit()

            except Exception as er:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write(log_time() + '\tfunc:' + func_name + '\tsearch\t' + search_word + '\tno result and save sql db log failed\t' + er + '\n')
                    mark_eye_01(2, imdb_id)
                    driver.quit()
                    return

        elif int(search_result_count) > 0:
            end = time.time()
            spent_time = int(end - start)
            row = (id, date, spent_time, imdb_id, search_word, search_result_count)
            try:
                # mark sql imdb_id column eye_01
                write_eye_01(row)
                print('write_eye_01 : sql save')
                try:
                    # save the page to  mongodb
                    html_page = driver.page_source
                    date_ymd = datetime.now().strftime('%Y-%m-%d')
                    doc = {"created_date": date_ymd, 'id': id, 'imdb_id': imdb_id, 'search_word': search_word, 'page': html_page}
                    mongo.insert_eye_search_result(doc)

                    # update scrape result into sql imdb_id
                    mark_eye_01(1, imdb_id)
                except Exception as mongo_er:
                    print('mongo_er_start')
                    with open(log_path, 'a', encoding='utf-8') as f:
                        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
                        f.write(current_time + '\t' + 'func_name ' + '\tmongo.insert_eye_search_result\t' +'imdb_id\t' + imdb_id + '\t'+ search_word + '\tinsert failed\t' + str(mongo_er) + '\n')
                    mark_eye_01(3, imdb_id)

            except Exception as er:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write(log_time() + '\tfunc:' + func_name + '\tsearch\t' + search_word + '\tmark sql log failed\t' + str(er) +'\n')
                    driver.quit()
                    mark_eye_01(2, imdb_id)
                    return

    except Exception as er:
        with open(log_path, 'a', encoding='utf-8') as f:
            print('final', er)
            f.write(log_time() + '\tfunc_name\t' + func_name + '\t' + imdb_id + '\t' + word + '\t' +str(er) + '\n')
            # empty page, not zero (word: Champagne!)
            mark_eye_01(2, imdb_id)
            print('save error status')
    driver.quit()
# eye_01('tt1049413', 'up')





