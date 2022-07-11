from bs4 import BeautifulSoup
import re
from urllib.parse import unquote
from package.db.mongo import get_mongo_eye_03_detail
from package.db.sql import write_main_info
from package.general import log_time

log_path = 'crawler/log/eye_04.log'

def str_to_date(str):
    year = str.split('/')[0]
    month = str.split('/')[1]
    day = str.split('/')[2]
    merge = year + '-'+ month +'-' + day
    return merge

def imdb_id_filter(url):
    home = 'https://imdb.com/Title?'
    id = 'tt' + url.replace(home, '').rstrip('/')
    return id


def clean_text(str):
    # remove title
    text_after = str.split('劇情簡介')[1].replace('\n', '').strip()
    # removie a tag url
    # remove_href = text_after
    remove_href = re.sub(r'href[^>]*', '', text_after).strip()
    return remove_href


# input: 友你才精彩 BEAUTIFUL MINDS
# output: 友你才精彩 , BEAUTIFUL MINDS
def eye_title_split(str, zh_title=False):
    str = str.strip()

    # can get zh_title from <title> href,  split text by zh_title
    if zh_title != False:
        en_title = str.replace(zh_title, '')
        return zh_title, en_title
    # split text by rule
    else:
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



# clean
def clean_one_detail_page(doc_dict):
    func_name = 'eye_04_clean_detail > clean_one_detail_page()'
    row = {}

    # EYE_ID
    eye_id = doc_dict['eye_id']
    print('eye_id', eye_id)

    # print(doc_dict)
    page = doc_dict['page']
    soup = BeautifulSoup(page, 'lxml')
    # print('soup', soup)

    # TITLE
    title_div = soup.find('div', {'class': 'filmTitle'})
    title = title_div.text.strip()
    try:
        title_unicode = soup.find('span', {'class': 'ratingbutton'}).find('a')['href'].split('filmtitle=')[1]  # 9A%E7%8D%A......
        zh_title = unquote(title_unicode)  # 捍衛戰士：獨行俠
        # print('zh_title', zh_title)
        zh_title, en_title = eye_title_split(title, zh_title=zh_title)
    except Exception as er:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(log_time() + '\tfunc_name\t' + func_name + '\t' + 'no title_unicode' + '\t' + str(er) + '\n')
        zh_title, en_title = eye_title_split(title, zh_title=False)

    # IMAGE
    try:
        image = soup.find('a',{'class': 'image Poster'})
        image_url = image.find('img')['src']
        print('image_url', image_url)
    except:
        image_url= None

    # VIDEO
    try:
        video = soup.find('iframe', {'class': 'image featured'})['src']
        # print('video', video)
    except:
        video = None

    # RUNTIME
    try:
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
        print('runtime', runtime, 'release_date', release_date)
    except:
        runtime = None
        release_date = None


    # stars tfdb
    stars = []
    imdb_id = None

    # TEXT
    try:
        story_div = str(soup.find('div', {'style': 'width:90%;font-size: 1.1em;'}) )
        story = clean_text(story_div)
        # print('story', story)
    except:
        story = None

    li_list = soup.find('div', {'id': 'filmCastDataBlock'}).find_all('li')

    star_begin = None
    for li in li_list:
        # print('li', li)
        try:
            li_a = li.find('a')
            li_a_href = li_a['href']
            li_id = li_a_href.lstrip('http://app2.atmovies.com.tw/star/').rstrip('/twweekend/').rstrip('/')
            # print('li_a_href', li_a_href)
            # print('li_id', li_id)

            if '導演' in li.text:
                zh_director = li_a.text.strip()
                director_tuple = (li_id, zh_director)
                row.update({'director': director_tuple})
                print('director_tuple', director_tuple)

            elif '編劇' in li.text:
                zh_writer = li_a.text
                writer_tuple = (li_id, zh_writer)
                row.update({'writer': writer_tuple})
                print('writer_tuple', writer_tuple)

            elif '演員' in li.text:
                star = li_a.text.strip()
                star_tuple = (li_id, star)
                stars.append(star_tuple)
                star_begin = 1
            elif star_begin == 1 and li.find_all('b') != []:
                star_begin = None
            elif star_begin == 1 and li.find_all('b') == []:

                if li_a.text not in ['MORE', 'more', 'More']:
                    star = li_a.text.strip()
                    star_tuple = (li_id, star)
                    stars.append(star_tuple)
                else:
                    star_begin = None

            elif 'IMDb' in li.text:
                imdb_id = imdb_id_filter(li_a_href)
                print('imdb_id', imdb_id)
        except Exception as er:
            with open('test.log', 'a', encoding='utf-8') as f:
                print('final', er)
                f.write(log_time() + '\tfunc_name\t' + func_name + '\t' + str(er) + '\n')


    if imdb_id != None:
        merge_id = imdb_id
    else:
        merge_id = eye_id

        print('-------------------------------')
    print('stars', stars)
    row.update(
        {'merge_id': merge_id, 'imdb_id': imdb_id, 'eye_id': eye_id, 'zh_title': zh_title, 'en_title': en_title, 'runtime': runtime,
     'release_date': release_date, 'video': video, 'text': story, 'star_dict': stars, 'image_url': image_url}
    )

    print('row', row)
    try:
        write_main_info(row)
    except Exception as er:
        print('Error Message', er)




















def eye_04_clean_detail(start, end):
    eye_3_detail_docs = get_mongo_eye_03_detail(start, end)
    for idx, doc in enumerate(eye_3_detail_docs):
        print('IDX', idx)
        clean_one_detail_page(doc)






eye_04_clean_detail('2022-07-07', '2022-07-07')
