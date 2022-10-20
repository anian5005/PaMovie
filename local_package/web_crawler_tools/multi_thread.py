# Standard library imports
import time
import threading
import random

# Third party imports
from time import sleep
from queue import Queue
from datetime import date
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Local application imports
from local_package.fetcher.proxy_fetcher import get_proxy_from_webshare
from local_package.db.mongodb import get_mongo_doc, create_mongo_connection
from local_package.db.mysql import MySQL, update_on_duplicate_key, save_processing_finished_log


sql = MySQL()


# used by class Multi_thread
class Worker(threading.Thread):
    def __init__(self, worker_no, queue, func, tool_box, table):
        threading.Thread.__init__(self)
        self.worker_no = worker_no
        self.job_queue = queue
        self.job_function = func
        self.error_count = 0
        self.tool_box = tool_box
        self.table = table
        self.data_list = []

    def run(self):
        ERROR_LIMIT = 5
        while self.job_queue.qsize() != 0 and self.error_count < ERROR_LIMIT:

            job_info = self.job_queue.get()
            job_idx = job_info['job_idx']
            input_id = job_info['input_id']
            self.do_job(job_idx=job_idx, input_id=input_id, tool_box=self.tool_box)
            if len(self.data_list) > 1000:
                self.save_data()
                self.data_list = []
            if self.error_count > ERROR_LIMIT:
                break
        # save the last data
        if self.data_list:
            self.save_data()
        # break loop when finished all job or greater than  ERROR_LIMIT

    def do_job(self, job_idx, input_id, tool_box):
        print(f"worker {self.worker_no} start job {job_idx}\n")
        return_result = self.job_function(**input_id, **tool_box)

        print(f"worker {self.worker_no} finish job {job_idx}\n")

        if type(return_result) == dict:
            # save dict data, e.g. {'imdb_id': 'tt0013030', 'douban_id': '5078750'}
            self.data_list.append(return_result)
        elif return_result == 'stop worker':
            self.error_count = 5  # STOP WORKER
        elif type(return_result) == int and return_result > 1:
            self.error_count = self.error_count + 1

    def save_data(self):
        if self.data_list:
            update_on_duplicate_key(connection=self.tool_box['sql_conn'], table=self.table, data_list=self.data_list,
                                    multi_thread=True)


class MultiThread(object):
    def __init__(self, id_list, worker_num, func, file_name, table, resource_list):
        self.id_list = id_list
        self.worker_num = worker_num
        self.job_queue = Queue()
        self.job_function = func
        self.file_name = file_name
        self.table = table
        self.resource_list = resource_list
        self.sql_conn_pool = sql.create_conn_pool()
        self.mongo_pool = create_mongo_connection()[1]

    def set_queue(self):
        for idx, input_id in enumerate(self.id_list):
            job_info = {'job_idx': idx, 'input_id': input_id}
            self.job_queue.put(job_info)

    def create_tool_box(self):
        tool_box = {}

        for item in self.resource_list:
            if item == 'sql_conn':
                sql_conn = self.sql_conn_pool.connect()
                tool_box[item] = sql_conn
            elif item == 'mongo_conn':
                mongo_conn = self.mongo_pool.movie
                tool_box[item] = mongo_conn
        return tool_box

    def close_resource(self):
        self.sql_conn_pool.dispose()
        self.mongo_pool.close()

    def create_worker(self):
        start = time.time()
        self.set_queue()
        workers = []
        # decrease worker num
        if len(self.id_list) < self.worker_num:
            self.worker_num = len(self.id_list)

        for i in range(self.worker_num):
            tool_box = self.create_tool_box()
            worker = Worker(
                worker_no=i,
                queue=self.job_queue,
                func=self.job_function,
                tool_box=tool_box,
                table=self.table
            )
            workers.append(worker)
            worker.start()

        for worker in workers:
            worker.join()

        # all job finished
        self.save_finished_log(start)
        self.close_resource()

    def save_finished_log(self, start):
        sql_conn = self.sql_conn_pool.connect()
        finished_job_count = len(self.id_list) - self.job_queue.qsize()
        log_info = {
            'connection': sql_conn,
            'start': start,
            'file_name': self.file_name,
            'func_name': self.job_function.__name__,
            'status_code': self.query_error_log(sql_conn, start)
        }
        save_processing_finished_log(finished_num=finished_job_count, log_info=log_info)
        sql_conn.close()
        self.sql_conn_pool.dispose()

    def query_error_log(self, sql_conn, start):
        func_name = self.job_function.__name__
        log_date = date.fromtimestamp(start)
        condition = """WHERE log_time > {} AND func_name ='{}'""".format(log_date, func_name)
        result = sql.fetch_data(conn=sql_conn, table='error_log', columns=['status_code'], struct='list', condition=condition)
        error_log_count = len(result)
        if error_log_count > 0:
            return 2
        else:
            return 1


class SeleniumWorker(Worker):
    def __init__(self, worker_no, queue, func, table, tool_box, proxy_list):
        Worker.__init__(self, worker_no, queue, func, tool_box, table)
        self.proxy_list = proxy_list
        self.proxy = self.proxy_list.pop()

    def tool_box_add_create_driver(self):
        user_agent = UserAgent()  # use_cache_server=False
        random_user_agent = user_agent.random
        webdriver.DesiredCapabilities.CHROME['proxy'] = {
            "sslProxy": self.proxy,
            "proxyType": "MANUAL"
        }
        options = Options()
        options.add_argument(f'user-agent={random_user_agent}')
        options.add_argument("start-maximized")
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        webdriver.DesiredCapabilities.CHROME['acceptSslCerts'] = True
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        # add driver into tool_box
        self.tool_box.update({'driver': driver})

    def run(self):
        ERROR_LIMIT = 5
        while self.job_queue.qsize() > 0 and self.error_count < ERROR_LIMIT and len(self.proxy_list) > 0:
            job_info = self.job_queue.get()
            job_idx = job_info['job_idx']
            input_id = job_info['input_id']
            status_code = self.do_job(job_idx=job_idx, input_id=input_id, tool_box=self.tool_box)
            sleep(10)
            # retry when anti-scraping
            if status_code == 4 or status_code == 5:
                self.do_job(job_idx=job_idx, input_id=input_id, tool_box=self.tool_box)


        self.save_data()
        # break loop when finished all job or greater than  ERROR_LIMIT

    def do_job(self, job_idx, input_id, tool_box):
        # create a driver, then add into tool_box
        self.tool_box_add_create_driver()

        print(f"worker {self.worker_no} start job {job_idx}\n")
        return_result = self.job_function(input_id, **tool_box)

        # status_code: 1: successful, 2: error, 4: ip blocked, 5: retry
        if type(return_result) == dict:
            self.data_list.append(return_result)

        elif return_result == 2:
            self.error_count = self.error_count + 1

        if type(return_result) != 4:
            # change new proxy
            self.proxy_list.append(self.proxy)
            self.proxy = self.proxy_list.pop()
            # save dict data, e.g. {'imdb_id': 'tt0013030', 'douban_id': '5078750'}

            print(f"worker {self.worker_no} finish job {job_idx}\n")

            return return_result  # status code: 1, 2, 5 or data dict

        elif return_result == 4:
            # anti-scraping, abandon the blocked proxy, then get new one
            print(self.proxy, 'this proxy was blocked')
            self.proxy = self.proxy_list.pop()
            return 4


# add driver & proxy_list
class SeleniumCrawler(MultiThread):
    def __init__(self, id_list, worker_num, func, file_name, table, resource_list):
        MultiThread.__init__(self, id_list, worker_num, func, file_name, table, resource_list)
        self.proxy_list = get_proxy_from_webshare()

    def create_worker(self):
        start = time.time()
        self.set_queue()
        workers = []

        # decrease worker num
        if len(self.id_list) < self.worker_num:
            self.worker_num = len(self.id_list)

        for i in range(self.worker_num):
            tool_box = self.create_tool_box()
            worker = SeleniumWorker(worker_no=i,
                                    queue=self.job_queue,
                                    func=self.job_function,
                                    proxy_list=self.proxy_list,
                                    tool_box=tool_box,
                                    table=self.table)
            workers.append(worker)
            worker.start()

        for worker in workers:
            worker.join()
        # all job finished
        self.save_finished_log(start)
        self.close_resource()
        print('proxy can use num', len(self.proxy_list))


class DoubaonWorker(Worker):
    def __init__(self, worker_no, queue, func, tool_box, table, proxy_list, coolies_list):
        Worker.__init__(self, worker_no, queue, func, tool_box, table)
        self.proxy_list = proxy_list
        self.proxy = self.proxy_list.pop()
        self.cookies_list = coolies_list

    def add_douban_cookies_into_toolbox(self):
        cookies = random.choice(self.cookies_list)
        # add cookies into tool_box
        self.tool_box.update({'douban_cookies_doc': cookies})

    def add_proxy_into_toolbox(self):
        # add proxy into tool_box
        self.tool_box.update({'proxy': self.proxy})

    def run(self):
        ERROR_LIMIT = 5
        while self.job_queue.qsize() != 0 and self.error_count < ERROR_LIMIT and len(self.proxy_list) > 0:
            sleep(40)
            job_info = self.job_queue.get()
            job_idx = job_info['job_idx']
            input_id = job_info['input_id']
            status_code = self.do_job(job_idx=job_idx, input_id=input_id, tool_box=self.tool_box)

            # retry when anti-scraping
            if status_code == 4 or status_code == 5:
                self.do_job(job_idx=job_idx, input_id=input_id, tool_box=self.tool_box)

        print('break loop')
        # save the last data
        self.save_data()
        # break loop when finished all job or greater than  ERROR_LIMIT

    def do_job(self, job_idx, input_id, tool_box):
        # get a cookies, then add into tool_box
        self.add_douban_cookies_into_toolbox()
        self.add_proxy_into_toolbox()

        print(f"worker {self.worker_no} start job {job_idx}\n")
        return_result = self.job_function(**input_id, **tool_box)

        # status_code: 1: successful, 2: error, 4: ip blocked
        if return_result != 4:
            # change new proxy
            self.proxy_list.append(self.proxy)
            self.proxy = self.proxy_list.pop()
        elif return_result == 4:
            # anti-scraping, abandon the blocked proxy, then get new one
            print(self.proxy, 'this proxy was blocked')
            try:
                self.proxy = self.proxy_list.pop()
                return 4
            # all proxy dead
            except:
                self.error_count = 5

        if type(return_result) == dict:
            # save dict data, e.g. {'imdb_id': 'tt0013030', 'douban_id': '5078750'}
            self.data_list.append(return_result)
            print(f"worker {self.worker_no} finish job {job_idx}\n")

        elif return_result == 2:
            self.error_count = self.error_count + 1


class DoubaonCookiesCrawler(MultiThread):
    def __init__(self, id_list, worker_num, func, file_name, table, resource_list):
        MultiThread.__init__(self, id_list, worker_num, func, file_name, table, resource_list)
        self.proxy_list = get_proxy_from_webshare()
        self.douban_cookies_list = self.get_cookies()

    def create_worker(self):
        start = time.time()
        self.set_queue()
        workers = []

        # decrease worker num
        if len(self.id_list) < self.worker_num:
            self.worker_num = len(self.id_list)

        for i in range(self.worker_num):
            tool_box = self.create_tool_box()
            worker = DoubaonWorker(worker_no=i,
                                   queue=self.job_queue,
                                   func=self.job_function,
                                   proxy_list=self.proxy_list,
                                   tool_box=tool_box,
                                   coolies_list=self.douban_cookies_list,
                                   table=self.table)
            workers.append(worker)
            worker.start()

        for worker in workers:
            worker.join()
        # all job finished
        self.save_finished_log(start)
        self.close_resource()

    def get_cookies(self):
        mongo_db = self.mongo_pool.movie
        docs = get_mongo_doc(db=mongo_db, collection_name='douban_cookies')
        cookies_list = [i for i in docs]
        return cookies_list
