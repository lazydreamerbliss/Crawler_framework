
import re
import sys
import datetime
import random
import time
import requests
import logging
import pybloom_live
from config import make_random_useragent, get_url_legal, CONFIG_URLPATTERN_ALL


class Fetcher:
    def __init__(self, max_repeat=3, sleep_time=0):
        """
        :param max_repeat: define counter of re-try when failure happens
        :param sleep_time: define a random sleep_time
        """
        self._max_repeat = max_repeat
        self._sleep_time = sleep_time

    def fetch_working(self, url: str, keys: object, repeat: int) -> (int, object):
        """
        :return (fetch_result, content): fetch_result can be
            -1 (fetch failed and reach max_repeat),
             0 (need repeat),
             1 (fetch success)
        """

        # FIXME
        logging.warning("%s Fetcher start: keys=%s, repeat=%s, url=%s", self.__class__.__name__, keys, repeat, url)

        time.sleep(random.randint(0, self._sleep_time))
        try:
            fetch_result, content = self.url_fetch(url)
        except Exception as E:

            # FIXME
            logging.warning("%s Fetcher ERROR: %s", self.__class__.__name__, E)

            if repeat >= self._max_repeat:
                fetch_result, content = -1, None
            else:
                fetch_result, content = 0, None

        # FIXME
        logging.warning("%s Fetcher end: fetch_result=%s, url=%s", self.__class__.__name__, fetch_result, url)

        return fetch_result, content

    def url_fetch(self, url:str) -> (int, object):
        response = requests.get(url,
                                headers={"User-Agent": make_random_useragent(),
                                         "Accept-Encoding": "gzip"},
                                timeout=(3.05, 10))
        return 1, (response.status_code, response.url, response.text)


class Parser:
    def __init__(self, max_deep=-1):
        """
        :param max_deep: define depth of crawler, -1 means infinite
        """
        self._max_deep = max_deep

    def parse_working(self, priority:int, url:str, keys:object, deep:int,
                      content:object) -> (int, list, list):
        """
        working function, must "try, except" and don't change the parameters
        and return: (parse_result, url_list, save_list), parse_result can be:
            -1 (parse failed),
             1 (parse success)

        url_list[] item format: (url, keys, priority)
        """

        # FIXME
        logging.warning("%s Parser start: priority=%s, keys=%s, deep=%s, url=%s", self.__class__.__name__, priority, keys, deep, url)

        try:
            parse_result, url_list, finger_print = self.html_parse(priority, url, keys, deep, content)
        except Exception as E:

            # FIXME
            logging.warning("%s Parser ERROR: %s", self.__class__.__name__, E)

            parse_result, url_list, finger_print = -1, [], []

        # FIXME
        logging.warning("%s Parser end: parse_result=%s, len(url_list)=%s, len(finger_print)=%s, url=%s", self.__class__.__name__, parse_result, len(url_list), len(finger_print), url)

        return parse_result, url_list, finger_print

    def html_parse(self, priority:int, url:str, keys:object, deep:int, content:object) -> (int, list, list):
        """
        parse the content of a url, you can rewrite this function, parameters
        and return refer to self.parse_working()

        A <a href> attribute specifies the link's destination, e.g:
            <a href="https://www.sample.com">Visitor</a>

        content = (response.status_code, response.url, response.text)
        """
        *_, html_text = content
        url_list = []

        if(self._max_deep < 0) or (deep < self._max_deep):
            # re.findall() returns a list of matched elements
            href_list = re.findall(r"<a[\w\W]+?href=\"(?P<url>[\w\W]{5,}?)\"[\w\W]*?>[\w\W]+?</a>",
                                   html_text, flags=re.IGNORECASE)
            url_list = [(_url, keys, priority+1)
                        for _url in [get_url_legal(href_tag, url) for href_tag in href_list]]

        # re.search() returns a MatchObject with matched locations in a string,
        # using .group() to get data
        title = re.search(r"<title>(?P<title>[\w\W]+?)</title>",
                          html_text,
                          flags=re.IGNORECASE)
        finger_print = [(title.group("title").strip(), datetime.datetime.now()), ] if title else []
        return 1, url_list, finger_print


class Saver:
    def __init__(self, pipe=sys.stdout):
        """
        :param pipe: define output method
        """
        self.pipe = pipe

    def save_working(self, url:str, keys:object, finger:(list, tuple)) -> bool:
        """
        :return save_result: True or False
        """

        # FIXME
        logging.warning("%s Saver start: keys=%s, url=%s", self.__class__.__name__, keys, url)

        try:
            save_result = self.item_saver(url, keys, finger)
        except Exception as E:

            # FIXME
            logging.warning("%s Saver ERROR: %s", self.__class__.__name__, E)

            save_result = False

        # FIXME
        logging.warning("%s Saver end: save_result=%s, url=%s", self.__class__.__name__, save_result, url)

        return save_result

    def item_saver(self, url: str, keys: object, finger: (list, tuple)) -> bool:
        if not isinstance(self.pipe, (str, list, tuple)):
            self.pipe.write("\t".join([url, str(keys)] + [str(i) for i in finger]) + "\n")
            self.pipe.flush
        else:
            finger_temp = [str(i) for i in finger]
            # finger_dict = {'url':url, 'Title':finger_temp[0], 'finger_temp':finger[1]}
            # with open(self._pipe+".json", "a", encoding='utf-8') as F:
            #     json.dump(finger_dict, F)
            finger_temp[0] = re.sub(r"\s+", " ", re.sub(r"&nbsp;|\n", "", finger_temp[0]))
            with open(self.pipe + ".json", "a", encoding='utf-8') as F:
                F.write("    {{\n      \"URL\":\"{}\",\n      \"TITLE\":\"{}\",\n      \"TIME\":\"{}\"\n    }},\n".format(url, finger_temp[0], finger_temp[1]))
        return True


class Filter:
    def __init__(self, black=(CONFIG_URLPATTERN_ALL,), white=("^http",), bloom_capacity=None):
        self._black_list = [re.compile(pattern, flags=re.IGNORECASE) for pattern in black] if black else []
        self._white_list = [re.compile(pattern, flags=re.IGNORECASE) for pattern in white] if white else []
        self._url_set = set() if not bloom_capacity else None
        self._bloom_filter = pybloom_live.ScalableBloomFilter(bloom_capacity, error_rate=0.001) if bloom_capacity else None

    def check(self, url):
        for b in self._black_list:
            if b.search(url):
                return False

        for w in self._white_list:
            if w.search(url):
                return True

    def check_repetition(self, url):
        """
        do repetition check based on url: if url is already in bloom_filter or
        in set, return False, else return True
        """

        # FIXME
        logging.warning("%s Filter start: checking url=%s", self.__class__.__name__, url)

        result = False
        if self.check(url):
            if self._url_set is not None:
                result = (url not in self._url_set)
                self._url_set.add(url)
            else:
                result = (not self._bloom_filter.add(url))

        #FIXME
        logging.warning("%s Filter end: finish checking url=%s, result=%s", self.__class__.__name__, url, str(result))

        return result

    def update(self, url_list):
        if self._url_set is not None:
            self._url_set.update(url_list)
        else:
            for url in url_list:
                self._bloom_filter.add(url)
