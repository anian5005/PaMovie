from queue import Queue
import threading
from package.db.sql import create_conn_pool, get_connection
from package.db.sql import save_processing_finished_log
from package.general import get_current_taiwan_datetime
from package.db.mongo import create_mongo_connection

from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from package.proxy_pool import get_raw_data_from_webshare
from package.db.mongo import get_mongo_doc
import random


# used by class Multi_thread
class Worker(threading.Thread):
    def __init__(self, conn, idx, queue, func):
        threading.Thread.__init__(self)
        self.sql_conn = conn
        self.worker_no = idx
        self.job_queue = queue
        self.job_function = func
        self.error_count = 0

    def run(self):
        ERROR_LIMIT = 5
        while self.job_queue.qsize() != 0 and self.error_count < ERROR_LIMIT:
            job_info = self.job_queue.get()
            job_idx = job_info['idx']
            url_id = job_info['url_id']
            self.do_job(self.sql_conn, job_idx, url_id)
        # break loop when finished all job or greater than  ERROR_LIMIT
        self.sql_conn.close()

    def do_job(self, sql_conn, job_idx, id):
        print(f"worker {self.worker_no} start job {job_idx}\n")
        status_code = self.job_function(sql_conn, id)
        # status_code: 2: error, 1: successful
        if status_code != 1:
            self.error_count = self.error_count + 1
        print(f"worker {self.worker_no} finish job {job_idx}\n")


class Multi_thread():
    def __init__(self, id_list, worker_num, func, file_name):
        self.file_name = file_name
        self.func = func
        self.sql_conn_pool = create_conn_pool()
        self.job_queue = Queue()
        self.all_jobs = 0
        self.id_list = id_list
        self.worker_num = worker_num
        self.job_function = func
        self.start_datetime = 0

    def set_queue(self):
        for idx, url_id in enumerate(self.id_list):
            self.job_queue.put({'idx': idx, 'url_id': url_id})
            self.all_jobs = self.job_queue.qsize()

    def create_worker(self):
        self.start_datetime = get_current_taiwan_datetime()
        self.set_queue()
        workers = []
        for i in range(self.worker_num):
            sql_conn = get_connection(self.sql_conn_pool)
            worker = Worker(conn=sql_conn, idx=i, queue=self.job_queue, func=self.job_function)
            workers.append(worker)
            worker.start()
        for worker in workers:
            worker.join()
        # all job finished
        self.save_finished_log()
        self.sql_conn_pool.dispose()

    def calculate_finished_job_count(self):
        finished_job_count = self.all_jobs - self.job_queue.qsize()
        return finished_job_count

    def save_finished_log(self):
        log_conn = get_connection(self.sql_conn_pool)
        finished_job_count = self.calculate_finished_job_count()
        func_name = self.func.__name__
        save_processing_finished_log(connection=log_conn, start=self.start_datetime, file_name=self.file_name, func_name=func_name, finished_num=finished_job_count)
        log_conn.close()
        self.sql_conn_pool.dispose()

class Mongo_worker(Worker):
    def __init__(self, sql_conn, mongo_conn, idx, queue, func):
        Worker.__init__(self, sql_conn, idx, queue, func)
        self.mongo_conn = mongo_conn

    def do_job(self, sql_conn, job_idx, doc):
        print(f"worker {self.worker_no} start job {job_idx}\n")
        status_code = self.job_function(sql_conn, self.mongo_conn, doc)
        # status_code: 2: error, 1: successful
        if status_code != 1:
            self.error_count = self.error_count + 1
        print(f"worker {self.worker_no} finish job {job_idx}\n")

    def run(self):
        ERROR_LIMIT = 5
        while self.job_queue.qsize() != 0 and self.error_count < ERROR_LIMIT:
            job_info = self.job_queue.get()
            job_idx = job_info['idx']
            doc = job_info['url_id']
            self.do_job(self.sql_conn, job_idx, doc)
        # break loop when finished all job or greater than  ERROR_LIMIT
        self.sql_conn.close()


class Mongo_multi_thread(Multi_thread):
    def __init__(self, id_list, worker_num, func, file_name):
        Multi_thread.__init__(self, id_list, worker_num, func, file_name)
        self.mongo_pool = create_mongo_connection()[1]

    def create_worker(self):
        self.start_datetime = get_current_taiwan_datetime()
        self.set_queue()
        print('job num:', self.all_jobs)
        workers = []
        for i in range(self.worker_num):
            sql_conn = get_connection(self.sql_conn_pool)
            mongo_conn = self.mongo_pool.movie
            worker = Mongo_worker(sql_conn=sql_conn, mongo_conn=mongo_conn, idx=i, queue=self.job_queue, func=self.job_function)
            workers.append(worker)
            worker.start()
        for worker in workers:
            worker.join()
        # all job finished
        self.save_finished_log()
        self.mongo_pool.close()


class Selenium_worker(Worker):
    def __init__(self, conn, idx, queue, func, driver):
        Worker.__init__(self, conn, idx, queue, func)
        self.driver = driver

    def do_job(self, sql_conn, job_idx, url_id):
        print(f"worker {self.worker_no} start job {job_idx}\n")
        status_code = self.job_function(sql_conn, url_id, self.driver)
        # status_code: 2: error, 1: successful
        if status_code != 1:
            self.error_count = self.error_count + 1
        print(f"worker {self.worker_no} finish job {job_idx}\n")


class Multi_thread_selenium_crawler(Multi_thread):
    def __init__(self, id_list, worker_num, func, file_name):
        Multi_thread.__init__(self, id_list, worker_num, func, file_name)
        self.driver_log = []
        self.proxy_list = get_raw_data_from_webshare()

    def create_driver(self, ip):
        user_agent = UserAgent(use_cache_server=False)
        random_user_agent = user_agent.random
        options = Options()
        options.add_argument("--proxy-server=http://" + ip)
        options.add_argument(f'user-agent={random_user_agent}')
        options.add_argument("start-maximized")
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        return driver

    def close_driver(self):
        for driver in self.driver_log:
            try:
                driver.close()
                driver.quit()
            except:
                pass

    def create_worker(self):
        self.start_datetime = get_current_taiwan_datetime()
        self.set_queue()
        workers = []
        self.driver_log = []
        for i in range(self.worker_num):
            sql_conn = get_connection(self.sql_conn_pool)
            ip = self.proxy_list[i]['ip']
            driver = self.create_driver(ip)
            self.driver_log.append(driver)
            worker = Selenium_worker(conn=sql_conn, idx=i, queue=self.job_queue, func=self.job_function, driver=driver)
            workers.append(worker)
            worker.start()
        for worker in workers:
            worker.join()
        # all job finished
        self.save_finished_log()
        self.close_driver()


class Beautifulsoup_worker(Mongo_worker):
    def __init__(self, sql_conn, mongo_conn, idx, queue, func, cookies_list, proxy_list):
        Mongo_worker.__init__(self, sql_conn, mongo_conn, idx, queue, func)
        self.cookies_list = cookies_list
        self.idx = idx
        self.proxy_list = proxy_list

    def run(self):
        ERROR_LIMIT = 5
        while self.job_queue.qsize() != 0 and len(self.proxy_list) > 0 and self.error_count < ERROR_LIMIT:
            job_info = self.job_queue.get()
            job_idx = job_info['idx']
            url_id = job_info['url_id']
            self.do_job(self.sql_conn, job_idx, url_id)
        # break loop when finished all job or greater than  ERROR_LIMIT or no proxy could use
        self.sql_conn.close()

    def do_job(self, sql_conn, job_idx, url_id):
        print(f"worker {self.worker_no} start job {job_idx}\n")
        cookies = random.choice(self.cookies_list)
        status_code = self.job_function(sql_conn, self.mongo_conn, url_id, cookies, self.proxy_list)
        # status_code: 2: error, 1: successful
        if status_code != 1:
            self.error_count = self.error_count + 1
        print(f"worker {self.worker_no} finish job {job_idx}\n")


class Beautifulsoup_multi_thread_crawler(Mongo_multi_thread):
    def __init__(self, id_list, worker_num, func, file_name):
        Mongo_multi_thread.__init__(self, id_list, worker_num, func, file_name)
        self.proxy_list = get_raw_data_from_webshare()
        self.cookies_list = self.get_cookies()

    def get_cookies(self):
        mongo_db, mongo_conn = create_mongo_connection()
        docs = get_mongo_doc(mongo_db, 'douban_cookies')
        mongo_conn.close()
        cookies_list = [i for i in docs]
        return cookies_list

    def create_worker(self):
        self.start_datetime = get_current_taiwan_datetime()
        self.set_queue()
        workers = []
        for worker_num in range(self.worker_num):
            mongo_conn = self.mongo_pool.movie
            sql_conn = get_connection(self.sql_conn_pool)
            worker = Beautifulsoup_worker(sql_conn, mongo_conn, worker_num, self.job_queue, self.job_function, self.cookies_list, self.proxy_list)
            workers.append(worker)
            worker.start()
        for worker in workers:
            worker.join()
        # all job finished
        self.save_finished_log()
        self.mongo_pool.close()
        self.sql_conn_pool.dispose()
        print('finally proxy_list', self.proxy_list)
