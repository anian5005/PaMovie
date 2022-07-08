from bs4 import BeautifulSoup

from package.db.mongo import get_mongo_eye_03_detail




def str_to_date(str):
    year = str.split('/')[0]
    month = str.split('/')[1]
    day = str.split('/')[2]
    merge = year + '-'+ month +'-' + day
    return merge


def clean_one_detail_page(doc_dict):
    # print(doc_dict)
    page = doc_dict['page']
    soup = BeautifulSoup(page, 'lxml')
    # print('soup', soup)

    # title # 缺斷詞規則
    title_div = soup.find('div', {'class': 'filmTitle'})
    title = title_div.text
    # video
    video = soup.find('iframe', {'class': 'image featured'})['src']
    # print('video', video)

    # runtime
    time_div = soup.find('ul', {'class': 'runtime'}).find_all('li')

    for li in time_div:
        # print(li.text)
        time_str = li.text

        # runtime
        if '片長' in time_str:
            runtime = time_str.replace('片長', '').lstrip('：').strip('分')

        # release_date
        elif '上映日期' in time_str:
            release_date = time_str.replace('上映日期', '').lstrip('：')  # 2022/06/18
            release_date = str_to_date(release_date)  # 2022-06-18
    # print('runtime', runtime, 'release_date', release_date)


    # stars tfdb
    stars = []
    zh_writer = None
    zh_director = None
    director_tuple = None
    writer_tuple = None

    li_list = soup.find('div', {'id': 'filmCastDataBlock'}).find_all('li')
    for i in li_list:
        star_begin = None
        print('li', li)
        li_a = li.find('a')
        li_a_href = li_a['href']
        li_id = li_a_href.lstrip('http://app2.atmovies.com.tw/star/').rstrip('/twweekend/').rstrip('/')
        # print('li_a_href', li_a_href)
        # print('li_id', li_id)


        # print('li_a.text', li_a.text, 'li_id', li_id)
        # http://www.atmovies.com.tw/movie/fbfr13553662/ -- > fbfr13553662
        if '導演' in li.text:
            zh_director = li_a.text
            star_id = li_id
            director_tuple = (star_id, zh_director)
            print('director_tuple', director_tuple)
        elif '編劇' in li.text:
            zh_writer = li_a.text
            star_id = li_id
            writer_tuple = (star_id, zh_writer)
            print('writer_tuple', writer_tuple)
        elif '演員' in li.text:
            star = li_a.text
            star_id = li_id
            star_tuple = (star_id, star)
            stars.append(star_tuple)
            star_begin = 1
        elif star_begin == 1 and li.find_all('b') != []:
            star_begin = None
        elif star_begin == 1 and li.find_all('b') == []:
            if li_a.text not in ['MORE', 'more', 'More']:
                star = li_a.text
                star_id = li_id
                star_tuple = (star_id, star)
                stars.append(star_tuple)
        print('-------------------------------')
# print('stars', stars)



















def eye_04_clean_detail(start, end):
    eye_3_detail_docs = get_mongo_eye_03_detail(start, end)
    for doc in eye_3_detail_docs:
        clean_one_detail_page(doc)
        break






eye_04_clean_detail('2022-07-06', '2022-07-06')
