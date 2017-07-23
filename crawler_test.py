
from threadpool import ThreadPool
from workers import Fetcher, Parser, Saver, Filter
import json
import pprint as pp

if __name__ == "__main__":

    url = "https://www.jetbrains.com/help/pycharm/commenting-and-uncommenting-blocks-of-code.html"
    fetcher = Fetcher()
    parser = Parser(max_deep=2)
    saver = Saver(pipe='test_result')
    filter = Filter(bloom_capacity=1000)

    spider = ThreadPool(fetcher, parser, saver, filter)
    spider.run(url, None, priority=0, deep=0)

    with open("test_result.json", "r", encoding='utf-8') as F:
        data = json.load(F)

    pp.pprint(data)

