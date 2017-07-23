
import queue
import threading
import enum
import logging


class FLAGS(enum.Enum):
    """
    status of crawler
    """
    TASKS_RUNNING = "tasks_running"     # flag of tasks_running
    URL_FETCH = "url_fetch"             # flag of url_fetched
    HTML_PARSE = "html_parse"           # flag of html_parsed
    ITEM_SAVE = "item_save"             # flag of item_saved
    URL_NOT_FETCH = "url_not_fetch"     # flag of url_not_fetch
    HTML_NOT_PARSE = "html_not_parse"   # flag of html_not_parse
    ITEM_NOT_SAVE = "item_not_save"     # flag of item_not_save


class BaseThread(threading.Thread):
    def __init__(self, name, worker, pool):
        threading.Thread.__init__(self, name=name)
        self._worker = worker     # Fetcher/Parser
        self._thread_pool = pool  # ThreadPool

    def run(self):

        # FIXME
        logging.warning("%s[%s] start...", self.__class__.__name__, self.getName())

        while True:
            try:  # if working() returns False, which means working() done
                if not self.working():
                    break
            except queue.Empty:
                if self._thread_pool.all_tasks_done():
                    break
            except Exception as E:
                break

        # FIXME
        logging.warning("%s[%s] end...", self.__class__.__name__, self.getName())


def start_fetch(self):
    priority, url, keys, deep, repeat = self._thread_pool.get_task(FLAGS.URL_FETCH)
    fetch_result, content = self._worker.fetch_working(url, keys, repeat)

    if fetch_result == 1:  # fetch success, change FLAG to HTML_PARSE for parsing
        self._thread_pool.update_dict(FLAGS.URL_FETCH, +1)
        self._thread_pool.add_task(FLAGS.HTML_PARSE, (priority, url, keys,
                                                      deep, content))
    elif fetch_result == 0:  # fetch failed, put back to Queue and repeat later
        self._thread_pool.add_task(FLAGS.URL_FETCH, (priority+1, url, keys,
                                                     deep, repeat+1))
    self._thread_pool.finish_task(FLAGS.URL_FETCH)
    return False if fetch_result == -1 else True

# Class of FetchThread (from BaseThread), with method alias "working()" to
# start_fetch()
FetchThread = type("FetchThread", (BaseThread,), dict(working=start_fetch))


def start_parse(self):
    priority, url, keys, deep, content = self._thread_pool.get_task(FLAGS.HTML_PARSE)
    parse_result, url_list, finger_print = self._worker.parse_working(priority, url, keys, deep, content)

    if parse_result > 0:
        self._thread_pool.update_dict(FLAGS.HTML_PARSE, +1)
        for _url, _keys, _priority in url_list:
            self._thread_pool.add_task(FLAGS.URL_FETCH, (_priority, _url,
                                                         _keys, deep+1, 0))
        for finger in finger_print:
            self._thread_pool.add_task(FLAGS.ITEM_SAVE, (url, keys, finger))
    self._thread_pool.finish_task(FLAGS.HTML_PARSE)
    return True

# Class of ParseThread (from BaseThread), with method alias "working()" to
# start_parse()
ParseThread = type("ParseThread", (BaseThread,), dict(working=start_parse))


def start_save(self):
    url, keys, finger = self._thread_pool.get_task(FLAGS.ITEM_SAVE)
    save_result = self._worker.save_working(url, keys, finger)

    if save_result:
        self._thread_pool.update_dict(FLAGS.ITEM_SAVE, +1)
    self._thread_pool.finish_task(FLAGS.ITEM_SAVE)
    return True

# Class of SaveThread (from BaseThread), with method alias "working()" to
# start_save()
SaveThread = type("SaveThread", (BaseThread,), dict(working=start_save))
