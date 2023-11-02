import utils
import threading
import requests
import logging
from utils import producer_retry
from xml.dom import minidom


# common variables shared across all the producer threads
producer_threads = []
poperty_list = ['D:getfileext', 'D:creationdate', 'D:displayname', 'D:getcontentlength', 'D:getlastmodified',
                'D:getsha1', 'D:getcontenttype', 'D:resourcetype', 'D:getlastaccessed',
                'D:getfilepath', 'D:getowner', 'D:getfilename']


class Producer:
    def __init__(self, is_filter, body, uri_scheme, uri_hostname, url, email, password):
        self.is_filter = is_filter
        self.filter_body = body
        self.scheme = uri_scheme
        self.hostname = uri_hostname
        self.url = url
        self.email = email
        self.password = password


    @staticmethod
    def is_present(ele, key):
        """
        Args:
            ele: DOM element object of given key e.g. type <DOM Element: D:xyz>
            key: string from property_list e.g. 'D:getcontentlength'
        Returns:
            if TAG is present in PROPFIND response and it contains data then val
            else returns None
            For D:resourcetype as 'file' it raises IndexError and we return string "file".
        """
        val = None
        if (ele.length > 0):
            try:
                val = ele[0].childNodes[0].nodeValue
            except IndexError as i:
                if key == 'D:resourcetype':
                    val = 'file'
                pass
            except Exception as e:
                print(e)
        return val


    @staticmethod
    def parse_propfind_response(responses):
        """
        Args:
            responses: PROPFIND response tags where each
        Returns:
            [] if responses <= 0
            if responses > 0 :
            sample output for file response is as follows
            (if some tags are not available for folder response it will be None)
            [{'D:href': '/webdav/user1/device1/' ,
              'D:status': 'HTTP/1.1 200 OK',
              'D:getfileext': 'DB',
              'D:creationdate': 'Wed, 08 Mar 2023 07:53:03 GMT',
              'D:displayname': 'file,v1.db',
              'D:getcontentlength': '7663616',
              'D:getlastmodified': 'Wed, 08 Mar 2023 08:49:40 GMT',
              'D:getsha1': '0097fbed8c8656c9077446bd9d9c9cbe580a140e',
              'D:getcontenttype': 'application/octet-stream',
              'D:resourcetype': '',
              'D:getlastaccessed': 'Wed, 08 Mar 2023 08:49:41 GMT',
              'D:getfilepath': 'D:\Download_Job_221\DB\file.db',
              'D:getowner': 'admin1',
              'D:getfilename': 'file.db'
            }]
        """
        response_list = list()
        if responses.length > 0:
            # when response tag is present in PROPFIND response
            for response in responses:
                parsed_response = dict()
                parsed_response["D:href"] = response.getElementsByTagName('D:href')[0].firstChild.nodeValue
                response_prop = response.getElementsByTagName('D:propstat')
                for x in response.getElementsByTagName('D:propstat'):
                    parsed_response["D:status"] = x.getElementsByTagName('D:status')[0].firstChild.nodeValue
                    prop = x.getElementsByTagName('D:prop')
                    for p in prop:
                        for myproperty in poperty_list:
                            parsed_response[myproperty] = Producer.is_present(p.getElementsByTagName(myproperty), myproperty)
                response_list.append(parsed_response)
        return response_list

    # number of retries 5 is, sleep time between retries is 2
    @producer_retry(5,2)
    def make_propfind(self, url,body=None):
        """
        Append the body of the request according to the filter used
        Args:
            url: Complete url of the form
                https://restore-c1-cloud.druva.com/webdav/lh/USER1/Dev1
            body: True indicates user wants to download filtered data using targeted API
                  False indicates all data without targeted filter. No body will be passed in PROPFIND call.
        """
        if body:
            # Propfind with filters
            utils.targeted_log("Making a propfind call to : %s with filter body." % url)
            response = utils.producer_session.request(method="PROPFIND",url=url,data=self.filter_body)
        else:
            #Propfind without filters
            utils.targeted_log("Making a propfind call to : %s without filter body" % url)
            response = utils.producer_session.request(method="PROPFIND", url=url)
        return response


    @staticmethod
    def is_parent(parent_url, resp):
        """
        :param parent_url: parent url.
        :param resp: resp is dict from parsed xml output for a folder or file.
        :return:
            True is parent_url path is same as resp['D:href'] else False
        """
        parent_path = utils.get_webdav_path(parent_url)
        return parent_path==resp['D:href']

    @staticmethod
    def add_candidates(parsed_output, download_entire_folder, parent_url, candidates):
        """
        This method add possible candidates folders for further processing.
        :param parsed_output: list of dicts for parsed xml output of depth 1 propfind reponse.
        :param download_entire_folder: True indicates entire folder should be downloaded.
        :param parent_url: parent url
        :param candidates: list of candidates folders
        :return:
        """
        for index, resp in enumerate(parsed_output):
            if Producer.is_parent(parent_url, resp):
                # skip root folder
                continue
            candidates.append((resp, download_entire_folder))

    def next_candidates(self, parsed_output_without_filters, parsed_output_with_filters, parent_url, download_entire_folder):
        """
        Args:
            parsed_output_without_filters: xml parsed output containing root, child1, child2 .....etc
                                            here child1 can be folder as welll as file.
            parsed_output_with_filters: xml parsed output containing root, child1, child2 .....etc
                                            here child1 can be folder as welll as file.
        Returns:
            candidates:
                list of folders which will be used in nect PROPDFIND call of depth 1.
        """
        candidates = []

        parsed_output_without_filters = [resp for resp in parsed_output_without_filters if resp['D:resourcetype'] != 'file']
        parsed_output_with_filters = [resp for resp in parsed_output_with_filters if resp['D:resourcetype'] != 'file']

        if self.is_filter and not download_entire_folder:
            Producer.add_candidates(parsed_output_with_filters,True,parent_url,candidates)
            if candidates == []:
                Producer.add_candidates(parsed_output_without_filters, download_entire_folder, parent_url, candidates)
        else:
            Producer.add_candidates(parsed_output_without_filters, download_entire_folder, parent_url, candidates)
        return candidates

    @staticmethod
    def add_files_to_queue(parsed_output):
        """
        :param parsed_output: list of dcists. xml parsed output containing root, child1, child2 .....etc
                            here child can be folder as well as file.
        :return:
        """
        for resp in parsed_output:
            if resp['D:resourcetype'] == 'file':
                with utils.lock:
                    utils.q.put((resp))

    def worker(self, url, download_entire_folder):
        """
        :param client: client object.
        :param webdav_path: webdav_path
        :return:
        """
        try:
            prop_resp = self.make_propfind(url)
            if self.is_filter and not download_entire_folder:
                prop_resp_with_filters = self.make_propfind(url, body=True)
        except Exception as e:
            utils.targeted_log("Skipping: WebDav url=%s, fault=%s" % (url, e) , logging.ERROR)
        else:
            responses_without_filters = minidom.parseString(prop_resp.text).getElementsByTagName('D:response')
            parsed_output_without_filters = Producer.parse_propfind_response(responses_without_filters)
            if self.is_filter and not download_entire_folder:
                responses_with_filters = minidom.parseString(prop_resp_with_filters.text).getElementsByTagName('D:response')
                parsed_output_with_filters = Producer.parse_propfind_response(responses_with_filters)
                # add files from parsed_output_with_filters to q for downloading
                Producer.add_files_to_queue(parsed_output_with_filters)
                candidates = self.next_candidates(parsed_output_without_filters, parsed_output_with_filters,url,download_entire_folder)
                [self.worker(utils.create_url(self.scheme, self.hostname, c['D:href']),flag) for (c,flag) in candidates]
            else:
                Producer.add_files_to_queue(parsed_output_without_filters)
                if parsed_output_without_filters:
                    for index,resp in enumerate(parsed_output_without_filters):
                        if Producer.is_parent(url, resp):
                            # skip root folder as it already processed
                            continue
                        if resp['D:resourcetype'] != 'file':
                            new_url = utils.create_url(self.scheme, self.hostname, resp['D:href'])
                            self.worker(new_url,download_entire_folder)


