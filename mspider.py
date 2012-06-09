# -*- coding: UTF-8 -*-

from fetchpage import *
import mthreadpool2
import sqlite3
import threading
import time
from optparse import OptionParser

__author__ = "shiweifu"
__mail__ = "shiweifu@gmail.com"
__license__ = "MIT"

class WatchThread(threading.Thread):
    """ 每隔十秒钟打印出信息 """
    def __init__(self, callparam):
        super(WatchThread, self).__init__()
        self.callparam = callparam
        self._dismissed = threading.Event()
        self.setDaemon(1)

    def dismiss(self):
        self._dismissed.set()

    def run(self):
        while True:
            if self._dismissed.isSet():
                break    
            print "总共有: ", len(self.callparam.url_list), "个链接"
            print "队列中剩余任务: ", str(self.callparam.request_queue.unfinished_tasks)
            print
            print 
            print
            time.sleep(10)

def spider(url, deeph, _db_name, log_name, log_level, _filter_key, thread_num):
    """ 通过传递进来的参数，开始执行爬虫任务 """
    request_queue = Queue.Queue(0)
    pool = mthreadpool2.ThreadPool(request_queue)
    #timeout给的大一点是为了防止网速慢的情况读不出来
    pool.create_workers(num_workers=thread_num, poll_timeout=60)
    db_name = _db_name
    if file_exist(db_name):
        os.remove(db_name)

    if file_exist(log_name):
        os.remove(log_name)

    sqliteCon = sqlite3.connect(db_name)
    sqliteCon.text_factory = str
    cur = sqliteCon.cursor()
    cur.execute(CREATE_TABLE_SQL)

    callparam = CallableParam()
    callparam.put_request = pool.put_request
    callparam.task = [url, 0, ""]
    callparam.deeph = deeph
    callparam.root_url = url
    callparam.filter_ext = FILTER_EXTS
    callparam.filter_key = _filter_key
    callparam.log_level = log_level
    callparam.logger = initlog(log_name)
    callparam.url_list = [callparam.root_url]
    callparam.sqlcur = cur
    callparam.request_queue = request_queue

    root_req = mthreadpool2.WorkRequest(_callable=down_page_callable, 
                                        _callback=write_log_callback, 
                                        _kwargs={"callparam":callparam})
    pool.put_request(root_req)

    print "共有", thread_num, "个线程"
    print "爬虫开始工作...参数:"
    print str(callparam)
    
    watch_thread = WatchThread(callparam)
    watch_thread.start()

    pool.poll()
    watch_thread.dismiss()
    count = len(callparam.url_list)
    callparam.logger.log(logging.DEBUG, get_format_log(total_url=str(count)))
    print count
    sqliteCon.commit()
    sqliteCon.close()


def testself():
    spider("http://m.baidu.com", 2, "test.db", "spider.log", 5, "", 10)

def main():
#    testself()
    usage = """例:\

python mspider.py -u http://m.baidu.com -d 2 -f logfile

如果你想开启自测，请:
python mspider.py --testself

参数介绍:

"""
    parser = OptionParser(usage)
    parser.add_option("-u", dest="url", type="string",
                      help="指定爬虫开始地址")
    parser.add_option("-d", dest="deep", type="int",
                      help="指定爬虫深度")
    parser.add_option("-f", dest="logfile", default="spider.log", type="string",
                      help="日志记录文件，默认spider.log")
    parser.add_option("-l", dest="loglevel", default="5", type="int",
                      help="日志记录文件记录详细程度，数字越大记录越详细，可选参数")
    parser.add_option("--thread", dest="thread", default="10", type="int",
                      help="指定线程池大小，多线程爬取页面，可选参数，默认10")
    parser.add_option("--dbfile", dest="dbfile", type="string",
                      help="存放结果数据到指定的数据库(sqlite)文件中")
    parser.add_option("--key", metavar="key", default="", type="string",
                      help="页面内的关键词，获取满足该关键词的网页，可选参数，默认为所有页面")
    parser.add_option("--testself", action="store_true", dest="testself", default=False,
                      help="程序自测，可选参数")
    (options, args) = parser.parse_args()
    if options.testself:
        testself()
        return
    try:
        assert options.url is not None
        assert options.deep is not None
        assert options.dbfile is not None
    except AssertionError:
        print parser.usage
        return
    url = options.url
    deeph = options.deep
    db_name = options.dbfile
    log_name = options.logfile 
    log_level = options.loglevel
    _filter_key = options.key
    thread_num = options.thread

    spider(url,
           deeph,
           db_name,
           log_name,
           log_level,
           _filter_key,
           thread_num)


if __name__ == '__main__':
    main()