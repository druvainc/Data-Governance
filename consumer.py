import threading
import time
import os
import utils
from consumer_utils import download_file

class ConsumerThread(threading.Thread):
    """
    ConsumerThread class which will create threads. Each thread will read from shared queue.
    File will be downloaded using the item from the queue.
    """
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        utils.LOGGER.info("initializing " + self.name)
        process_data(self.name, self.q)
        utils.LOGGER.info("Exiting " + self.name)

def wait_for_queue_empty():
    """
    Wait for the queue to empty
    """
    while not utils.q.empty():
        pass

def start_consumer_threads(no_of_threads):
    """
    Creates no_of_threads many threads to download files.
    Args:
        no_of_threads: number of consumer(file downloading threads)
    """
    for t in range(no_of_threads):
        threadID = t+1
        thread_name = 'thread'+str(threadID)
        thread = ConsumerThread(threadID, thread_name, utils.q)
        thread.start()
        utils.consumer_threads.append(thread)


def wait_for_consumer_threads():
    """
    wait for all consumer threads to finish
    """
    map(lambda t: t.join(), utils.consumer_threads)

def notify_consumer_threads():
    """
    Notify Consumer threads it's time to exit
    """
    utils.thread_exit_Flag = 1



def process_data(threadName, q):
    """
    helper function to process queue item(file metadata)
    Args:
        threadName: thread name
        q: shared queue between producer and consumer
    """
    while not utils.thread_exit_Flag:
        utils.lock.acquire()
        if not utils.q.empty():
            item = q.get()
            session = utils.get_new_session(utils.arguments.email, utils.arguments.password)
            utils.lock.release()
            uri_scheme, uri_hostname = utils.get_hostname(utils.arguments.url)
            url = utils.create_url(uri_scheme, uri_hostname,item['D:href'])
            file_path,folder_path = utils.create_file_path(utils.abs_path,item['D:href'])
            utils.setup_directory(folder_path)
            download_file(session, url, file_path, threadName, item["D:creationdate"],
                          item["D:getlastmodified"], item["D:getlastaccessed"])
        else:
            utils.lock.release()
            time.sleep(5)
