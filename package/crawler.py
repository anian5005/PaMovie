import re
from package.nlp import preprocess
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager

def fuzzy_name(str):
    result_list = []
    original = str
    # print('original_name', original)

    # replace space
    full_str = original.replace('  ', ' ').strip().strip('!').strip('！')
    clean_str = preprocess(full_str)
    result_list.append(full_str)

    # split comma
    comma = r':|：'
    if re.search(comma, full_str):
        # print('check', re.search(comma, full_str))
        split_str = re.split(comma, full_str)
        for str in split_str:
            str = str.lstrip(' ').rstrip(' ')
            result_list.append(str)

    # print(result_list)
    return result_list


def get_url(driver, url):
    driver.get(url)
    return driver

def regular_year(str):
    match = re.match(r'.*([(][1-3][0-9]{3}[)])', str)
    year = match.group(1).lstrip('(').rstrip(')')
    return year


def get_selenium_cookies():
    # create driver
    user_agent = UserAgent(use_cache_server=False)
    random_user_agent = user_agent.random
    options = Options()
    options.add_argument(f'user-agent={random_user_agent}')
    options.add_argument("start-maximized")
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    url = 'https://movie.douban.com/subject/10001432/'
    driver.get(url)
    cookies = driver.get_cookies()
    cookies_dict = {}
    for item in cookies:
        cookies_dict[item['name']] = item['value']
    driver.quit()
    return cookies_dict
