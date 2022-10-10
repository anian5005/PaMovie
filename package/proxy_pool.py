import requests
from db_setting.connect_set import proxy

proxy_url = proxy.set['url_with_key']


def my_address_ip():
    response = requests.get("https://httpbin.org/ip")
    print(response.text)


def get_proxy_from_webshare():
    webshare_proxy_list = []
    api = proxy_url
    response = requests.get(api)
    soup_list = str(response.text).split("\r\n")
    for i in soup_list[:100]:
        temp = i.split(':')
        ip = temp[0]
        port = temp[1]
        webshare_proxy_list.append(ip + ':' + port)
    return webshare_proxy_list

