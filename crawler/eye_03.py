from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from time import sleep
import requests
from datetime import datetime

from package.general import log_time
from package.db.s3 import put_photo_to_s3
from package.db.sql import mark_eye_02, get_eye_id_from_sql
from package.db.mongo import insert_mongo_eye_03_movie_page




log_path = 'log/eye_03.log'

# output: save movie page to mongo
# status code: {1:ok, 2: error, 3: save without img} (save in eye_02)
def scrape_movie_page(eye_id):

    func_name = 'scrape_movie_page'

    # driver config
    url = 'http://www.atmovies.com.tw/movie/' + eye_id
    user_agent = UserAgent()
    random_user_agent = user_agent.random
    options = Options()
    options.add_argument(f'user-agent={random_user_agent}')
    options.add_argument("start-maximized")
    options.add_argument("--window-size=1920,1080")
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
                mark_eye_02(2, eye_id)
                driver.quit()
                return
            else:
                driver.quit()
                sleep(3)
                pass

    # driver the page
    for i in range(1, 6):
        try:
            driver.get(url)
            break
        except:
            if i == 5:
                mark_eye_02(2, eye_id)
                driver.quit()
                return
            else:
                sleep(1)
                pass

    # Wait to load the page
    for i in range(1, 6):
        try:
            print('try')
            # print('Loading Page')
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "footer"))
            )
            break
        except requests.exceptions.RequestException as e:
            print('except')
            if i == 5:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write(log_time() + '\t' + 'func: ' + func_name + '\t' + eye_id + '\tloading page to scrape failed\t' + e +'\n')
                    mark_eye_02(2, eye_id)
                    driver.quit()
                    return
            else:
                pass

        except Exception as er:
            print('except 2')
            print('Error Message', er)
            if i == 5:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write(log_time() + '\t' + 'func: ' + func_name + '\t' + '\tloading searchForm to scrape failed\t' + er +'\n')
                    driver.quit()
                    mark_eye_02(2, eye_id)
                    return
            else:
                pass

    # save page to mongo
    html_page = driver.page_source
    # print('html_page', html_page)
    date_ymd = datetime.now().strftime('%Y-%m-%d')
    doc = {"created_date": date_ymd, 'eye_id': eye_id, 'page': html_page}

    try:
        # save page html into mongo eye_03_detail
        insert_mongo_eye_03_movie_page(doc)

        # wait for img, and download it
        for t in range(1, 5):
            print('t try img', t)
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'image.Poster'))
                )
                try:
                    # save img to s3
                    if driver.find_elements(By.CLASS_NAME, 'image.Poster'):
                        image = driver.find_element(By.CLASS_NAME, 'image.Poster')
                        if image.find_elements(By.TAG_NAME, 'img'):
                            img_element = image.find_element(By.TAG_NAME, 'img')
                            photo_name = eye_id + '_main.jpg'
                            put_photo_to_s3(photo_name=photo_name, photo_stream=img_element.screenshot_as_png)
                            mark_eye_02(1, eye_id)
                except Exception as e:
                    print('try except s3')
                    # save html page, but save img failed
                    mark_eye_02(3, eye_id)
                    print(e)

                break
            except Exception as er:
                print('try img er')
                if t == 5:
                    print('try if er')
                    with open(log_path, 'a', encoding='utf-8') as f:
                        f.write(
                            log_time() + '\t' + 'func: ' + func_name + '\tscrape\t' + eye_id + '\tloading input field failed\t' + er + '\n')
                        driver.quit()
                        mark_eye_02(3, eye_id)
                        return
                else:
                    print('try else er')
                    sleep(2)
                    pass

    except Exception as e:
        # save page html into mongo failed
        # save html failed
        print('e : ', e)
        mark_eye_02(2, eye_id)


scrape_movie_page('f1en00326716')


# get eye_id from sql, scrape movie page, and save html data to mongo
# status code: {1:ok, 2: error} (save in eye_02)
def eye_03_scrape(start_date, end_date):

    func_name = 'eye_03_scrape'

    #  get eye_id by data range
    for i in range(1, 4):
        try:
            eye_id_list = get_eye_id_from_sql(start_date, end_date)
            break
        except Exception as e:
            if i == 3:
                with open(log_path, 'a', encoding='utf-8') as f:
                    f.write(
                        log_time() + '\t' + 'func: ' + func_name + '\t' + 'get_eye_id_from_sql failed' + '\t' + e + '\n')
                return
            else:
                pass

    for eye_id in eye_id_list:
        try:
            # scrape html and saves mongo
            scrape_movie_page(eye_id)
        except:
            mark_eye_02(2, eye_id)
