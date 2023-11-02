import logging
import queue
import time, os, sys
import urllib.parse
import threading
import shutil
import requests
from argparse import ArgumentParser
from argparse import ArgumentTypeError
import argparse
from requests.adapters import HTTPAdapter, Retry

q = queue.Queue() # shared queue to put items between producer(single thread) and consumer threads
LOGGER = None
abs_path = ""
lock = threading.Lock() # lock for acquiring the queue to manage concurrent threads trying to put/get item from/to queue.
thread_exit_Flag = 0  # Flag to indicate
consumer_threads = [] # Consumer threads to download files
arguments = None

no_of_files_downloaded = 0
no_of_files_download_failed = 0
no_of_bytes_downloaded = 0
files_download_failed = dict()

producer_session = None
producer_session_count = 1


def producer_retry(num_of_retries, sleep_time):
    def decorator(func):
        def wrapper( *args, **kwargs):
            global producer_session, producer_session_count
            attempts = 0
            while attempts < num_of_retries:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.ConnectionError as cerr:
                    attempts += 1
                    #targeted_log("connection reset error before session=")
                    producer_session = get_new_session(arguments.email, arguments.password)
                    producer_session_count += 1
                    #targeted_log("connection reset error after session=")
                    if attempts == num_of_retries:
                        raise cerr
                    time.sleep(sleep_time)
                except Exception as err:
                    attempts += 1
                    #targeted_log("retry attempt no:%s"%attempts)
                    if attempts == num_of_retries:
                        raise err
                    time.sleep(sleep_time)
        return wrapper
    return decorator


def get_new_session(username, password):
    """
    Args:
        username: admin email id
        password: admin password
    Returns:
        session object
    """
    #session_retry_count = 5
    session = requests.Session()
    session.auth = (username, password)
    #session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def setup_directory(location):
    """
    Create directory for downloading data. If it exists then remove it.
    :param location: location at which download will happen.
    :return:
    """
    try:
        if not os.path.exists(location):
            os.makedirs(location)
    except FileExistsError:
        print("folder {} already exists".format(location))
    else:
        print("folder '{}' created ".format(location))


def targeted_log(message, level="info"):
    """
    Info level and error level logger setup.
    """
    global LOGGER
    if level == "info":
        LOGGER.info(message)
        print(message)
    elif level == logging.ERROR:
        LOGGER.error(message)
        print("ERROR: " + message)
    else:
        pass

def set_up_logger(download_location):
    """
    Function to setup logger.
    Logger would be set in the download_location/targeted_download_timestamp directory.
    Name of the file would be targeted_download_timestamp.log
    Timestamp is in %Y%m%d_%H%M%S format.
    """
    global LOGGER, abs_path
    current_timestamp = time.strftime("%Y%m%d_%H%M%S")
    folder_name = "targeted_download" + current_timestamp
    abs_path = os.path.join(download_location, folder_name)
    setup_directory(abs_path)
    log_location = abs_path + "/targeted_download_" + current_timestamp + ".log"
    if os.path.exists(log_location):
        os.remove(log_location)
    LOGGER = logging.getLogger(abs_path)
    LOGGER.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(log_location)
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)

def str2bool(v):
    """
    :param v: check and convert v value into True or False.
    :return: True or False
    """
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise ArgumentTypeError('Invalid value. Use True/yes/t/y/1 or False/no/f/n/0')

def parse_args():
    """
    1. email (str) - email of the admin(required)
    2. password (str)- password of the admin(required)
    3. url (str)- restore URL(required)
    4. filter (str) -
        To include the extensions : ext_include(optional)
        To exclude the extensions : ext_exclude(optional)
        To filter the data by name of the file/folder : name(optional)
        To get the data from the root directory : root_dir_name(optional)
      All these keys would take a comma separated string of file extensions (For e.g "py,txt,pdf")
    """
    global arguments
    parser = ArgumentParser(usage="Listing Files")
    required_args = parser.add_argument_group('required arguments')
    optional_args = parser.add_argument_group('optional arguments')

    required_args.add_argument("--email",
                               help="User Email",
                               type=str,
                               default=None,
                               required=True
                               )
    required_args.add_argument("--url",
                               help="Webdav Download URL(device level URL and it should end with / for collection.)",
                               type=str,
                               default=None,
                               required=True)
    required_args.add_argument("--password",
                               help="User Password",
                               type=str,
                               default=None,
                               required=True)

    exclude_obj = optional_args.add_argument( "--ext_exclude",
                                              help="List of extensions to be excluded(comma separated string e.g. py,txt)",
                                              type=str,
                                              default=None,
                                              required=False)
    include_obj = optional_args.add_argument( "--ext_include",
                                              help="List of extensions to be included(comma separated string e.g. png,jpeg)",
                                              type=str,
                                              default=None,
                                              required=False)

    list_only_folders_obj = optional_args.add_argument( "--list_only_folders",
                                                        type=str,
                                                        default=None,
                                                        required=False,
                                                        help="List of only folders(e.g. yes/true/t/y/1 or no/false/f/n/0)"
                                                        )

    match_any_one_obj = optional_args.add_argument( "--match_any_one",
                                                    type=str,
                                                    default=None,
                                                    required=False,
                                                    help="yes/true/t/y/1 indicates OR operation. no/false/f/n/0 indicates AND operation.(e.g. true/false)"
                                                    )

    name_obj = optional_args.add_argument( "--name",
                                           help="List of names of file/folders(Case sensitive substring matching of string.)",
                                           type=str,
                                           default=None,
                                           required=False)
    root_dir_name_obj = optional_args.add_argument( "--root_dir_name",
                                                    help="Name of the root directory(Case sensitive Exact matching of string.)",
                                                    type=str,
                                                    default=None,
                                                    required=False)
    optional_args.add_argument( "--location",
                         help="Download location. [default(current working directory): %(default)s]",
                         type=str,
                         default=os.getcwd(),
                         required=False
                         )


    mutually_exclusive_group1 = parser.add_mutually_exclusive_group()
    mutually_exclusive_group1._group_actions.append(list_only_folders_obj)
    mutually_exclusive_group1._group_actions.append(include_obj)
    mutually_exclusive_group1._group_actions.append(exclude_obj)

    mutually_exclusive_group2 = parser.add_mutually_exclusive_group()
    mutually_exclusive_group2._group_actions.append(match_any_one_obj)
    mutually_exclusive_group2._group_actions.append(list_only_folders_obj)

    arguments = parser.parse_args()
    if arguments.match_any_one:
        if not arguments.name:
            raise argparse.ArgumentError(match_any_one_obj, "name filter argument is necessary for using match_any_one filter.")
        if not (arguments.ext_include or arguments.ext_exclude):
            raise argparse.ArgumentError(match_any_one_obj, "At least one of ext_include or ext_exclude filter argument is necessary for using match_any_one filter.")


    return arguments

def create_xml_body(options):
    """
    Append the body of the request according to the filter used
    """
    body = '<?xml version="1.0" encoding="utf-8" ?>'
    body += '<D:propfind xmlns:D="DAV:">'
    body += "<D:allprop/>"
    body += "<D:filters>"
    if options.ext_include:
        body += "<D:ext_include>"
        body += options.ext_include
        body += "</D:ext_include>"
    if options.ext_exclude:
        body += "<D:ext_exclude>"
        body += options.ext_exclude
        body += "</D:ext_exclude>"
    if options.name:
        body += "<D:name>"
        body += options.name
        body += "</D:name>"
    if options.root_dir_name:
        body += "<D:root_dir_name>"
        body += options.root_dir_name
        body += "</D:root_dir_name>"
    if options.match_any_one:
        body += "<D:match_any_one>"
        body += str(str2bool(options.match_any_one))
        body += "</D:match_any_one>"
    if options.list_only_folders:
        body += "<D:list_only_folders>"
        body += str(str2bool(options.list_only_folders))
        body += "</D:list_only_folders>"
    body += "</D:filters>"
    body += "</D:propfind>"
    return body

def get_webdav_path(url):
    """
    Example url - https://restore-c1-cloud.druva.com/webdav/lh
    :param url: input url
    :return: webdavpath - "/webdav/lh"
    """
    obj = urllib.parse.urlparse(url, allow_fragments=False)
    webdav_path = obj.path
    if not webdav_path:
        targeted_log("Path for resource not found in URL",logging.ERROR)
        sys.exit(1)
    return webdav_path


def get_hostname(url):
    """
    Example url - https://restore-c1-cloud.druva.com/webdav/lh
    :param url: input url
    :return: hostname - "restore-c1-cloud.druva.com"
    """
    obj = urllib.parse.urlparse(url, allow_fragments=False)
    hostname = obj.netloc
    if not hostname:
        targeted_log("Hostname not found in URL", logging.ERROR)
        sys.exit(1)
    return obj.scheme, hostname


def is_filter_present(options):
    """
    Args:
        options: parsed input arguments.
    Returns: boolean value
    True - if at least one filter is applied
    False - no filter is applied
    """
    if options.ext_include or options.ext_exclude or options.name or options.root_dir_name:
        return True
    return False


def create_file_path(download_location,href):
    """
    creates absolute folder and file path.
    Args:
        download_location: path where data will be downloaded.
                            e.g. D:/target_dowbload_timestamp/data
        href:
            webdav url path e.g. /webdav/lh1/us1/dev1/folder1/abc.db
    Returns:
        file_path:    e.g.  D:/target_dowbload_timestamp/Data/lh1/us1/dev1/folder1/abc.db
        folder_path:  e.g.  D:/target_dowbload_timestamp/Data/lh1/us1/dev1/folder1
    """
    href_components = href.split("/")
    # href component 1 will be '' and component 2nd will be webdav
    # we do not want to create folder named webdav so drop these components
    href_components = [requests.utils.unquote(comp) for comp in href_components[2:]]
    file_name = href_components[-1]
    file_path = os.path.join(download_location,*href_components)
    folder_path = os.path.join(download_location,*href_components[:-1]) # except filename
    return file_path, folder_path

def create_url(uri_scheme, uri_hostname, href):
    """
    Args:
        uri_scheme: https
        uri_hostname: "restore-c1-cloud.druva.com"
        href: /webdav/lh/USER1/D1
    Returns:
        URL https://restore-c1-cloud.druva.com/webdav/lh/USER1/D1
    """
    return '{0}://{1}{2}'.format(uri_scheme, uri_hostname, href)
