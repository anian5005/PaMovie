import requests
import time


def my_address_ip():
    response = requests.get("https://httpbin.org/ip")
    print(response.text)


def get_raw_data_from_webshare():
    webshare_proxy_list = []
    api = 'https://proxy.webshare.io/proxy/list/download/ntxeoovsvpxgfdhbonhvbeijjlsbmbznvwpgnkqo/-/http/port/direct/'
    response = requests.get(api)
    # soup = BeautifulSoup(response.content, "html.parser")
    soup_list = str(response.text).split("\r\n")
    for i in soup_list[:9]:
        temp = i.split(':')
        ip = temp[0]
        port = temp[1]
        webshare_proxy_list.append(ip + ':' + port)
    # ['45.142.28.83',,...]
    print('webshare proxy finished')
    return webshare_proxy_list


# spy-one api update ip per hour
class free_proxy_list_from_spyone:
    def __init__(self):
        self.spyone_proxy_list = []
        self.requests_time_limit = 2

    def get_raw_data_from_spyone(self, ssl=False):
        url = 'https://spys.me/proxy.txt'
        response = requests.get(url)
        if response.status_code != 200:
            return None
        else:
            rows = response.text.split('\n')
            for item in rows[6:]:
                # item 3.37.142.199:3128
                slices = item.split(' ')
                proxy = slices[0]
                if len(slices) < 2:
                    # the last line mark e.g. '\r'
                    break
                proxy_info_list = slices[1].split('-')
                pos_mapping_info = {0: 'countryCode', 1: 'anonymity', 2: 'SSL'}
                proxy_info = {'proxy': proxy}
                for idx, i in enumerate(proxy_info_list):
                    key = pos_mapping_info[idx]
                    proxy_info[key] = i
                # proxy_info {'proxy': '140.227.59.167:3180', 'countryCode': 'JP', 'anonymity': 'N', 'SSL': 'S'}
                # https & country filter
                if ssl == True:
                    if 'SSL' in proxy_info:
                        self.spyone_proxy_list.append(proxy_info['proxy'])
                else:
                    self.spyone_proxy_list.append(proxy_info['proxy'])
        print('spyone finished')

    def test_ssl_proxy(self, ip):
        try:
            response = requests.get("https://httpbin.org/ip", proxies={'https': ip}, timeout=self.requests_time_limit)
            if response.status_code == 200:
                if response.json()['origin'] == ip.split(':')[0]:
                    return True
        except:
            return False
        return False

    def get_proxy_list(self):
        start = time.time()
        new_proxy_list = []
        self.get_raw_data_from_spyone(ssl=True)
        print('total num: ', len(self.spyone_proxy_list))
        for idx, ip in enumerate(self.spyone_proxy_list):
            print('ip idx', idx)
            if self.test_ssl_proxy(ip):
                new_proxy_list.append(ip)
        end = time.time()
        print('spent', end - start)
        return new_proxy_list
