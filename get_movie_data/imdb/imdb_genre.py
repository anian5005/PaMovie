import requests

from bs4 import BeautifulSoup
from local_package.db.mysql import insert_dict_list_into_db
from local_package.db.mysql import MySQL


# get imdb genre
def crawl_imdb_genre():
    sql = MySQL()
    engine, connection = sql.get_connection()

    url = 'https://help.imdb.com/article/contribution/titles/genres/GZDRMS6R742JRGAG#'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    article__second_section_div = soup.find('div', {'class': 'article__second_section'})
    p_div = article__second_section_div.find('p')
    a_list = p_div.findAll('a')

    new_list = []
    for a_tag in a_list:
        genre = a_tag.text.replace('\n', '').strip().replace(' ', '')
        new_list.append({'genre': genre})
    insert_dict_list_into_db(connection=connection, table_name='genre_type', dict_list=new_list, ignore=None)

    connection.close()
    engine.dispose()
