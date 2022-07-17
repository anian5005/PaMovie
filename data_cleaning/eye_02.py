from package.db.mongo import get_mongo_eye_1
from bs4 import BeautifulSoup
import time
from package.db.sql import write_eye_02_id
from package.general import log_time


# input: /F/fden03113456/
def extract_eye_id(str):
    href = str.split('/')
    for piece in href:
        if len(piece) > 8:
            eye_id = piece
            return eye_id
    return None


# get pages from movie.eye_1_search_result, clean data and save eye_id into sql

# input: time range & documents
# output: eye_id list
def eye_02_clean_page(start_date, end_date):

    log_path = '../crawler/log/eye_02.log'
    func_name = 'eye_02_clean_page'
    start = time.time()

    documents = get_mongo_eye_1(start_date, end_date)  # cursor
    eye_id_set = set()
    count = 0

    for idx, doc_dict in enumerate(documents):
        count = count + 1

        #doc_dict {'_id': ObjectId('62c458ff32af7195bd0aa923'), 'created_date': '2022-07-05', 'id': '0705232929', 'imdb_id': 'tt0326716', 'keyword': "'77", 'page': '<html>...}

        page = doc_dict['page']  # type str
        soup = BeautifulSoup(page, 'lxml')  # <class 'bs4.BeautifulSoup'>


        header_list = soup.find_all('header')
        for header in header_list:
            try:
                type = header.find('font', {"color": "#FF0000"})
                # find movie card
                if type.text == '電影':
                    print('type', type.text)
                    # find a tag
                    try:
                        a_tag = header.find('a')
                        # print('a_tag', a_tag)  #  <a class="title big" href="/F/fhen41392170/">飢餓遊戲 The Hunger Games </a>
                        href = a_tag['href']
                        eye_id = extract_eye_id(href)  # fren42419284
                        print(eye_id)
                        eye_id_set.add(eye_id)

                    except:
                        print('no href')
            except:
                print('no movie type')
            print('---------------')

    # make tuple list
    tuple_list = [(start_date, end_date, eye_id) for eye_id in eye_id_set]
    try:
        write_eye_02_id(start_date, end_date, tuple_list)
    except Exception as e:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(
                log_time() + '\t' + 'func: ' + func_name + '\t' + e + '\n')
    except:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(
                log_time() + '\t' + 'func: ' + func_name + '\t' + 'save write_eye_02_id failed' + '\n')



    end = time.time()
    print('doc_num', count)
    print('eye_id_set', eye_id_set)
    print('spent s', end-start)
    log_str = 'clean finished and save sql ' + 'doc_num ' + count + 'spent ' + str(end-start)
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(
            log_time() + '\t' + 'func: ' + func_name + '\t' + log_str + '\n')

# eye_02_clean_page('2022-07-05', '2022-07-05')


