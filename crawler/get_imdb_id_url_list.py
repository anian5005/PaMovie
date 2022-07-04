from datetime import datetime
import time
import json, re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import mysql.connector
from selenium import webdriver
import sys
# import step_1_get_imdb_id_V2
from webdriver_manager.chrome import ChromeDriverManager

log_path = 'crawler.log'

def get_imdb_url_list(url, count):
    next = 0
    start = time.time()

    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    # options.add_argument('--disable-gpu')
    # options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=options)

    driver.get(url)
    while driver.find_elements(By.XPATH, "//a[@class='lister-page-next next-page']"):
        next_link = driver.find_element(By.XPATH, "//a[@class='lister-page-next next-page']").get_attribute('href')
        # print('next_link', next_link)
        with open("url_list", 'a', encoding='utf-8') as f:
            f.write(str(count) + '\t' + url + '\n')
            count = count +1

                driver.get(next_link)


        else:
            with open("url_list", 'a', encoding='utf-8') as f:
                next ==1
                now = datetime.now()
                end = time.time()
                spent_time = end - start
                current_time = now.strftime('%Y-%m-%d %H:%M:%S')
                f.write(current_time + '\tget_imdb_url_list\t' + 'finish all page and get\t' + str(count) + '\tmovie urls' + '\tspent\t' + spent_time + '\n')
                print('finished')


url = 'https://www.imdb.com/search/title/?title_type=feature,tv_movie,tv_special,documentary,short,video&after=WzEzMjgxMSwidHQ4ODIxNzE4Iiw5Njc1MV0%3D&ref_=adv_nxt'
count =8788
get_imdb_url_list(url, count)


# def imdb_id_multi_thread(url):
#     # set search language in header
#     language_options = {'us': 'en-us,en;q=0.8,de-de;q=0.5,de;q=0.3', 'zh': 'zh-TW,zh;q=0.9'}
#     language_set = language_options['zh']
#     options = webdriver.ChromeOptions()
#     options.add_argument("start-maximized")
#     options.add_argument('--headless')
#     options.add_argument('--disable-gpu')
#     options.add_argument('--no-sandbox')
#
#     options.add_experimental_option('prefs', {'intl.accept_languages': language_set})
#     driver = webdriver.Chrome(options=options)
#     now = datetime.now()
#
#     try:
#         driver.get(url)
#         # Wait to load the page
#         WebDriverWait(driver, 5).until(
#             EC.presence_of_element_located((By.CLASS_NAME, "article"))
#         )
#     except:
#         # if page not exist
#         with open(log_path, 'a', encoding='utf-8') as f:
#             current_time = now.strftime('%Y-%m-%d %H:%M:%S')
#             f.write(current_time + '\timdb_id_V2()\t' + '\tloading page to scrape failed' + '\tlink\t' + url + '\n')
#         pass
#
#     if driver.find_elements(By.CLASS_NAME, 'article'):
#         article_div = driver.find_element(By.CLASS_NAME, 'article')
#         movie_lists = article_div.find_elements(By.CLASS_NAME, 'lister-item.mode-advanced')
#
#         for movie_div in movie_lists:
#             if movie_div.find_elements(By.CLASS_NAME, 'lister-item-header'):
#                 item_header = movie_div.find_element(By.CLASS_NAME, 'lister-item-header')
#                 if item_header.find_elements(By.TAG_NAME, 'a'):
#                     header_a = item_header.find_element(By.TAG_NAME, 'a')
#                     movie_url = header_a.get_attribute('href')
#                     # print('movie_url', movie_url)
#                     imdb_id = re.search(r'tt\d+', movie_url).group()
#                     # print('imdb_id', imdb_id)
#                     # (id, id_type, title, original_title_name, year)
#                     tuple = step_1_get_imdb_id_V2.movie_simple_info(imdb_id)
#                     print(tuple)
#                     # print('tuple', tuple)
#                     try:
#                         step_1_get_imdb_id_V2.write_imdb_movie_id(tuple)
#                         # print('------------------------')
#                     except:
#                         with open('imdb_id.txt', 'a', encoding='utf-8') as f:
#                             current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
#                             f.write(current_time + '\tfunc: get_imdb_movie_id\t' + 'imdb_id\t' + imdb_id + '\twrite to movie.imdb_movie_id failed\n')
#                 else:
#                     with open(log_path, 'a', encoding='utf-8') as f:
#                         current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
#                         f.write(current_time + '\tfunc: imdb_id\t' + 'get imdb_id, title, og_title, year failed \n')
#
#     else:
#         with open(log_path, 'a', encoding='utf-8') as f:
#             now = datetime.now()
#             current_time = now.strftime('%Y-%m-%d %H:%M:%S')
#             f.write(current_time + '\timdb_id_V2()\t' + '\tlocated class_name article failed' + '\tlink\t' + url + '\n')