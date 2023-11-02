import requests
import utils
import logging
import os
from datetime import datetime
from requests.adapters import HTTPAdapter, Retry

CHUNK_SIZE = 4 * 1024 * 1024 # 4MB chunks
EPOCH_TIME = datetime(1970, 1, 1)

def download_file(session, url, file_path, threadName, file_ctime, file_mtime, file_atime):
    """
    Args:
        session: session object.
        url: webdav url of given file to be downloaded.
        file_path: absolute file path where data will be saved
        threadName: thread name downloading this file
    """
    foff = 0
    headers = {"Range": "bytes=" + str(foff) + "-" + str(foff + CHUNK_SIZE - 1)}
    last = False
    retry_count=0
    status = "failure"
    utils.targeted_log("% s File download started = % s" % (threadName, url))
    curr_bytes_downloaded = 0
    while not last:
        try:
            # HTTP get request with the Range header
            response = session.request(method="GET", url=url, headers=headers)
            response_size_in_bytes = 0
            if "Content-Length" in response.headers:
                response_size_in_bytes = int(response.headers['Content-Length'])
            else:
                #print("size of response.content=",len(response.content))
                response_size_in_bytes =len(response.content)
            curr_bytes_downloaded += response_size_in_bytes
            with open(file_path, "ab") as f:
                f.write(response.content)
                retry_count = 0
            if response_size_in_bytes != CHUNK_SIZE:
                last = True
                status = "success"
            else:
                foff = foff + CHUNK_SIZE
                headers = {"Range": "bytes=" + str(foff) + "-" + str(foff + CHUNK_SIZE - 1)}
        except Exception as fault:
            retry_count += 1
            utils.targeted_log("% s While downloading = % s, WebDav fault = % s, retry count = % d" %
                               (threadName, url, str(fault), retry_count), logging.ERROR)
            if retry_count > 5:
                last = True
                status = "failure"

    if status == "success":
        with utils.lock:
            utils.no_of_files_downloaded += 1
            utils.no_of_bytes_downloaded += curr_bytes_downloaded
        utils.targeted_log("% s File downloaded successfully = % s" % (threadName, url))
        apply_md_timestamps(file_path, file_ctime, file_mtime, file_atime, threadName, url)
    else:
        with utils.lock:
            utils.no_of_files_download_failed+=1
            utils.files_download_failed[url] = curr_bytes_downloaded
        utils.targeted_log("% s File download failed = % s" % (threadName, url), logging.ERROR)
        utils.targeted_log("% s Skipping applying timestamps = % s" % (threadName, url))

def convert_time_string_to_int(time_string):
    """
    This function converts time in string to UNIX epoch time at seconds granularity.
    :param time_string: time in string in the format "%a, %d %b %Y %H:%M:%S %Z"
                        e.g. Fri, 10 Jul 2015 10:53:02 GMT
    :return: time_since_epoch as integer or None
    """
    time_since_epoch = None
    try:
        utc_time = datetime.strptime(
            time_string,
            "%a, %d %b %Y %H:%M:%S %Z"
        )
        time_since_epoch = (utc_time - EPOCH_TIME).total_seconds()
    except Exception as e:
        time_since_epoch = None
    return time_since_epoch


def set_timestamps(file_path, ctime, mtime, atime):
    """

    :param file_path: absolute file path
    :param ctime: file creation time
    :param mtime: file modified time
    :param atime: file last access time
    :return:
    """
    # Using twice update birth time
    # https://www.freebsd.org/cgi/man.cgi?query=utimes&sektion=2
    os.utime(file_path, (atime, ctime))
    os.utime(file_path, (atime, mtime))


def apply_md_timestamps(file_path, file_ctime, file_mtime, file_atime, threadName, url):
    """
    this function applies modified,access and creation time to downloaded file.
    :param file_path: absolute final path where file is downloaded.
    :param file_ctime: file creation time.
    :param file_mtime: file modified time.
    :param file_atime: file access time.
    """
    try:
        file_ctime_since_epoch = convert_time_string_to_int(file_ctime)
        file_mtime_since_epoch = convert_time_string_to_int(file_mtime)
        file_atime_since_epoch = convert_time_string_to_int(file_atime)

        if (file_atime_since_epoch is None) or (file_mtime_since_epoch is None):
            st = os.stat(file_path)
            if file_atime_since_epoch is None:
                file_atime_since_epoch = int(st.st_atime)
            if file_mtime_since_epoch is None:
                file_mtime_since_epoch = int(st.st_mtime)

        if file_ctime_since_epoch is None:
            file_ctime_since_epoch = file_mtime_since_epoch

        file_ctime_since_epoch = int(file_ctime_since_epoch)
        file_mtime_since_epoch = int(file_mtime_since_epoch)
        file_atime_since_epoch = int(file_atime_since_epoch)

        set_timestamps(file_path, file_ctime_since_epoch, file_mtime_since_epoch, file_atime_since_epoch)
    except Exception as fault:
        utils.targeted_log("% s Couldn't apply timestamps. fault: %s"% (threadName, fault), logging.ERROR)
    else:
        utils.targeted_log("% s Done applying timestamps %s." % (threadName, url))

