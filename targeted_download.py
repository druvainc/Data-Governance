"""
Script to consume Targeted Downloads API

   1. Include extension filter = ext_include
   2. Exclude extension filter = ext_exclude
   3. Name Filter = name
   4. Root Dir Filter = root_dir_name
   5. List only folders = list_only_folders
   6. Match any one filter =  match_any_one

Combinations of the filters can be used.
   For eg. ext_include + name + root_dir_name, ext_exclude + name, name + root_dir_name etc.

   (ext_include + ext_include) is not allowed.
   (ext_include + list_only_folders) is not allowed.
   (ext_include + list_only_folders) is not allowed.
   (Only match_any_one) is not allowed.
   (match_any_one + name) is not allowed.
   (match_any_one + name + ext_include + ext_exclude) is not allowed.

 Prerequisites to run this script
   1. Make sure that python3 or above is installed(tested on 3.9)
   2. modules to be installed
       python -m pip install requests

Input Parameters
   1. email (str) - email of the admin
   2. password (str)- password of the admin
   3. url (str)- restore URL
   4. To include the extensions : ext_include
      To exclude the extensions : ext_exclude
      To filter the data by name of the file/folder : name
      To get the data from the root directory : root_dir_name
      To list only folders : list_only_folders
      To match any one : match_any_one


 Usage
 python3 targeted_download_script.py --help

   Sample command to run the script :

   python targeted_download.py --email="john.carter@druva.org" --password="password"
   --url="https://restore-c123-qamicro.drtst.org/webdav/policy1/user1/dev1/"
   --ext_include="txt,py" --name="filename" --root_dir_name="folder123"
"""

import os, sys, time
import requests
import utils
import logging
from consumer import ConsumerThread, start_consumer_threads, notify_consumer_threads, \
    wait_for_consumer_threads, wait_for_queue_empty
from producer import Producer
from datetime import datetime
from consumer_utils import  download_file

no_of_threads = 5 # no of consumer threads

if __name__ == "__main__":
    try:
        # Parse the arguments
        arguments = utils.parse_args()
        filter_body = utils.create_xml_body(arguments)
    except Exception as fault:
        print("Provided input arguments were invalid: Error:. ",fault)
        sys.exit(0)

    try:
        # Setting up the logger
        utils.set_up_logger(arguments.location)
        utils.abs_path = os.path.join(utils.abs_path, "Data")
        utils.setup_directory(utils.abs_path)
        print(utils.abs_path)
    except Exception as fault:
        utils.targeted_log("Logger setup failed : %s. "% fault, logging.ERROR)
        sys.exit(0)


    start_time = datetime.now()
    utils.targeted_log("Downloading started at %s ..." % str(start_time))
    uri_scheme, uri_hostname = utils.get_hostname(arguments.url)

    # start consumer threads for file download
    start_consumer_threads(no_of_threads)

    #fill the queue using the producer worker
    is_filter = utils.is_filter_present(arguments)
    print(filter_body)
    p_obj = Producer(is_filter, filter_body, uri_scheme, uri_hostname, arguments.url, arguments.email, arguments.password)
    utils.producer_session = utils.get_new_session(arguments.email, arguments.password)
    p_obj.worker(arguments.url,False)

    wait_for_queue_empty()
    notify_consumer_threads()
    wait_for_consumer_threads()
    end_time = datetime.now()

    time.sleep(30)
    utils.targeted_log("Download completed at = % s." % str(end_time))
    utils.targeted_log("No of files downloaded = % s, No of files failed to download = % s, No of bytes downloaded in GB = % s"
                       % (utils.no_of_files_downloaded, utils.no_of_files_download_failed, str(utils.no_of_bytes_downloaded/(1024.0 * 1024.0 * 1024.0))))

    print("producer_session_count=",utils.producer_session_count)

    if utils.files_download_failed:
        time.sleep(15)
        utils.targeted_log("***************List of failed file download****************")
        for url, bytes_dowloaded in utils.files_download_failed.items():
            utils.targeted_log("url = % s, bytes_downloaded = % s" %(url,bytes_dowloaded))