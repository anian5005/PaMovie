import threading
import queue
from eye_01 import eye_01_scrape_by_id
from package.db.sql import init_db, get_cursor




def eye_01_multi_thread(year):

    class Worker(threading.Thread):
        def __init__(self, worker_num):
            threading.Thread.__init__(self)
            self.worker_num = worker_num

        def run(self):
            while job_queue.qsize() != 0:
                print('still left jobs :', job_queue.qsize())
                self.do_job(job_queue.get())

        def do_job(self, imdb_dict):
            print(f"worker {self.worker_num} start job {imdb_dict['idx']}")
            eye_01_scrape_by_id(imdb_dict['idx'], imdb_dict['dict'])
            print(f"worker {self.worker_num} finish job {imdb_dict['idx']}")

    def put_list_into_queue(list):
        for job_index in list:
            job_queue.put(job_index)

    # step 0 : create queue
    job_queue = queue.Queue()

    # step 1 : get list
    connection = init_db()
    cursor = get_cursor(connection, opt=True)
    sql = "SELECT * FROM movie.imdb_movie_id  WHERE type ='Movie' and year = %s and eye_01 <=> NULL;"
    cursor.execute(sql, (year,))
    connection.commit()
    imdb_dict_list = cursor.fetchall()
    imdb_dict_list = [ {'idx': idx, 'dict': dict} for idx, dict in enumerate(imdb_dict_list)]

    job_num = len(imdb_dict_list)
    print('Toatal job num', job_num)


    # step 2 : put list into queue
    if job_num > 0:
        put_list_into_queue(imdb_dict_list)

        # step 3 : create worker
        workers = []
        worker_count = 8  # We have 4 workers

        for i in range(worker_count):
            worker = Worker(i + 1)
            workers.append(worker)

        # Do the job
        for worker in workers:
            worker.start()

        for worker in workers:
            worker.join()

        print("Done.")
    else:
        print("no jobs need to do")








for year in range(1990, 2021):
    print('year', year)
    eye_01_multi_thread(year)