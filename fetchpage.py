# -*- coding: UTF-8 -*-
"""
爬虫
"""
import re
import BeautifulSoup
import urllib2
import Queue
import time
import httplib
import socket
from copy import copy
import mthreadpool2
import logging
import sqlite3
import os

__author__ = "shiweifu"
__mail__ = "shiweifu@gmail.com"
__license__ = "MIT"

CREATE_TABLE_SQL = """create table data (
  id INTEGER PRIMARY KEY  AUTOINCREMENT  ,
  url text,
  key text,
  html text
)"""

INSERT_DATA_SQL = """insert into data (url,key,html) values (?,?,?)"""

class TimeOut(Exception):
    """use urllib open web fail with timeout"""
    pass

class ParamsError(Exception):
    """ 传递进来的参数不够 """
    pass        

class ParsePageError(Exception):
    """ 解析页面时遇到错误 """

class FiltedPage(Exception):
    """ 过滤掉页面 """

FILTER_EXTS = [".exe", ".rar", ".zip", ".apk", ".mp3", ".sisx", ".sis"]

def urlopen_with_timeout(url, data=None, timeout=10):
    """ 设置socket超时,然后urllib2打开一个页面读取并返回 
    >>> import urllib
    >>> ret = urlopen_with_timeout("http://m.baidu.com")
    >>> print isinstance(ret, urllib.addinfourl)
    True
    """
    class TimeoutHTTPConnection(httplib.HTTPConnection):
      def connect(self):
        msg = "getaddrinfo returns an empty list"
        for res in socket.getaddrinfo(self.host, self.port, 0,
                        socket.SOCK_STREAM): 
          af, socktype, proto, canonname, sa = res
          try:
            self.sock = socket.socket(af, socktype, proto)
            if timeout is not None:
              self.sock.settimeout(timeout)
            if self.debuglevel > 0:
              print "connect: (%s, %s)" % (self.host, self.port)
            self.sock.connect(sa)
          except socket.error, msg:
            if self.debuglevel > 0:
              print 'connect fail:', (self.host, self.port)
            if self.sock:
              self.sock.close()
            self.sock = None
            continue
          break
        if not self.sock:
          raise socket.error, msg
  
    class TimeoutHTTPHandler(urllib2.HTTPHandler):
      http_request = urllib2.AbstractHTTPHandler.do_request_
      def http_open(self, req):
        return self.do_open(TimeoutHTTPConnection, req)
  
    opener = urllib2.build_opener(TimeoutHTTPHandler)
    try:
      body = opener.open(url, data)
    except UnicodeEncodeError:
      body = opener.open(url.encode("utf8"), data)
    except urllib2.URLError:
      raise TimeOut
    except urllib2.HTTPError:
      raise urllib2.HTTPError
    return body

def _page_contain_key(page, key):
    """页面是否包含关键词
    >>> page = urllib2.urlopen("http://m.baidu.com").read()
    >>> print _page_contain_key(page, "百度")
    True
    >>>
    """
    if key == "": #不过滤
        return False
    key = key.join(("|", ""))
    soup = BeautifulSoup.BeautifulSoup(page)
    if key:
        try:
            key = unicode(key, "utf8")
        except Exception:
            pass
        re_string = key
        if soup.findAll(text=re.compile(re_string)):
            return True
    return False

def initlog(fileName="logfile"):
    """ 初始化logger对象使用函数内的 fmt 作为格式 
    >>> import logging
    >>> ret = initlog("abc")
    >>> print isinstance(ret, logging.RootLogger)
    True
    """
    logger = logging.getLogger()
    hdlr = logging.FileHandler(fileName)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.NOTSET)
    return logger


class LogQueues(object):
    """ LogQueues 初始化时保存多个队列，用户在写入日志的时候不需要区分日志等级，直接根据不同等级进行写入
    只需要在写入文件的时候根据等级参数来获取不同队列即可. """
    def __init__(self, level):
        super(LogQueues, self).__init__()
        self._log_queues = []
        for _log_queues in xrange(0, level+1):
            self._log_queues.append(Queue.Queue(0))
        
    def __getattr__(self, attr):
        level = attr.split("level")[1]
        if re.match("\d+", level) is None:
            raise AttributeError
        level = int(level)
        return self._log_queues[level-1]

class PageResult(object):
    """ 页面获取完毕返回时返回的对象.
        filted表示该页面是否因为包含过滤词被过滤.
        log_queue是日志对象
    """

    def __init__(self, _log_level):
        super(PageResult, self).__init__()
        self.log_level = _log_level
        self.log_queues = LogQueues(self.log_level)
        self.page = ""
        self.log_level = _log_level
        self.filted = False
        self.errored = False

def write_log_callback(request, result):
    """ 回调函数,下载完网页时候由线程池调用 """
    assert isinstance(result, PageResult)
    level_index = 0
    callparam = request.kwargs["callparam"]
    logger = callparam.logger
    while level_index < result.log_level:
        level_attr = str(level_index+1).join(["level", ""])
        level_queue = getattr(result.log_queues, level_attr)
        while True:
            try:
                log = level_queue.get(False)
                logger.log(int(log[0]), log[1])
            except Queue.Empty:
                break
        level_index += 1
    if result.filted == True:
        return
    url = callparam.task[0]
    filter_key = callparam.filter_key
    cur = callparam.sqlcur
    cur.execute("insert into data (url,key,html) values (?,?,?)",
                                   (url, filter_key, result.page))

class CallableParam(object):
    """ 传递down_page_callable的参数 """
    put_request = None
    task = None
    deeph = None
    root_url = None
    filter_ext = None
    filter_key = None
    log_level = None
    logger = None
    url_list = None

    def __str__(self):
        
        assert getattr(self, "task") is not None
        assert getattr(self, "deeph") is not None
        assert getattr(self, "root_url") is not None
        assert getattr(self, "filter_ext") is not None
        assert getattr(self, "filter_key") is not None
        assert getattr(self, "log_level") is not None

        task_s = " ".join(("task:", str(self.task)))
        deeph_s = " ".join(("deeph:", str(self.deeph)))
        root_url_s = " ".join(("root url:", self.root_url))
        filter_ext_s = " ".join(("filter ext:", str(self.filter_ext)))
        filter_key_s = " ".join(("filter key:", self.filter_key))
        log_level_s = " ".join(("log level:", str(self.log_level)))


        return "\n".join(( 
                    task_s,
                    deeph_s,
                    root_url_s,
                    filter_ext_s,
                    filter_key_s,
                    log_level_s
                ))
        
def _get_page(_url):
    """ GET访问url并返回页面的文本 
    >>> page = _get_page("http://m.baidu.com")
    >>> print isinstance(page, str)
    True
    """
    url = _url
    try:
        page = urlopen_with_timeout(url).read()
    except TimeOut:
        raise TimeOut
    except urllib2.HTTPError:
        raise urllib2.HTTPError
    return page

def _process_page_url(href_tag, _callparam):
    """ 处理页面上的链接 """
    #判断是否是root
    if href_tag['href'] == "/":
        return False
    #相对地址
    if href_tag['href'].startswith('/'):
        href_tag['href'] = "".join((_callparam.root_url, href_tag['href']))


    if href_tag["href"] in _callparam.url_list:
        return False
    else:
        for ext in _callparam.filter_ext:
            if href_tag["href"].endswith(ext) == True:
                return False
    return True

def _parse_page(_page, _callparam):
    """ 解析页面，返回连接列表 """
    task_list = [] 
    _task = _callparam.task
    is_filter = False
    try:
        soup = BeautifulSoup.BeautifulSoup(_page)
    except Exception:
        raise ParsePageError

    urlall = soup.findAll('a', onclick=None, 
                          href=re.compile('^http:|^/'))
    url = _task[0]     
    if url.endswith('/'):
        url = url[:-1]

    for i in urlall:
        if _task[1] >= _callparam.deeph:
            return task_list

        if _process_page_url(i, _callparam) == False:
            continue

        _callparam.url_list.append(i['href'])
        task_list.append((i['href'], _task[1]+1, _task[0]))
    return task_list

def get_format_log(**dic):
    """遍历dic，合并kv成为最终字符串
    >>> print get_format_log(a="abc")
    a:abc
    """
    _log = ""
    for key in dic.keys():
        tmp = "".join((key, ":", dic[key]))
        _log = " ".join((_log, tmp))
    return _log.strip()


def file_exist(path):
    """ 判断路径的文件是否存在 
    >>> print file_exist("/")
    True
    """
    try:
        fstat = os.stat
    except NameError:
        import os
        fstat = os.stat
    try:
        fstat(path)
    except OSError:
        return False
    return True

def down_page_callable(**kw):
    """ 提供给线程池的回调函数，参数保存在kw["callparam"]中，必须为CallableParam类型。 
        在本函数中写入日志到日志队列，函数执行完毕后返回，由线程池调用回调函数。
    """
    callparam = kw["callparam"]
    assert isinstance(callparam, CallableParam)
    url = callparam.task[0]
    pr = PageResult(5) #共有五种日志级别

    pr.log_queues.level5.put((logging.INFO, 
                              get_format_log(param="\n"+str(callparam))))
    pr.log_queues.level2.put((logging.DEBUG, get_format_log(url=url)))
    try:
        page = _get_page(url)
    except TimeOut:
        pr.log_queues.level3.put((logging.WARN, 
                                  get_format_log(func="_get_page",
                                  error="timeout")))
        pr.errored = True
        return pr
    except urllib2.HTTPError:
        log = get_format_log(func="_get_page", 
                             error="urllib.HTTPError")
        pr.log_queues.level1.put((logging.ERROR, log))
        pr.errored = True
        return pr
    except socket.error:
        pr.errored = True
        log = get_format_log(func="_get_page", 
                             error="socket error")
        pr.log_queues.level1.put((logging.ERROR, log))
        return pr
    except Exception:
        pr.errored = True
        log = get_format_log(func="_get_page", 
                             error="unknow error")
        pr.log_queues.level1.put((logging.ERROR, log))
        return pr

    if _page_contain_key(page, callparam.filter_key) == True:
        log = get_format_log(error="page contain filter key")
        pr.log_queues.level1.put((logging.ERROR, log))
        pr.filted = True
        return pr
    pr.page = page
    try:
        task_list = _parse_page(page, callparam)
    except ParsePageError:
        log = get_format_log(func="_parse_page", 
                             error="ParsePageError")
        pr.log_queues.level1.put((logging.ERROR, log))
        pr.error = True
        return pr
    for _task in task_list:
        tmp = copy(callparam)
        tmp.task = _task
        req = mthreadpool2.WorkRequest(_callable=down_page_callable, 
                                       _callback=write_log_callback, 
                                       _kwargs={"callparam":tmp})
        callparam.put_request(req)
    return pr

