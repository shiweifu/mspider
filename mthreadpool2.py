# -*- coding: UTF-8 -*-

import sys
import threading
import Queue
import traceback

__author__ = "shiweifu"
__mail__ = "shiweifu@gmail.com"
__license__ = "MIT"

class WorkerThread(threading.Thread):
    """ 配合MThreadPool 和Queue 模块使用的线程 """
    def __init__(self, requests_queue, results_queue, poll_timeout=5, **kwds):
        threading.Thread.__init__(self, **kwds)
        self.setDaemon(1)
        self._requests_queue = requests_queue
        self._results_queue = results_queue
        self._poll_timeout = poll_timeout
        self._dismissed = threading.Event()
        self.start()

    def run(self):
        """ 不停的从队列中获得工作,直到所有任务完成或收到停止信号 """
        while True:
            if self._dismissed.isSet():
                break
            try:
                request = self._requests_queue.get(True, self._poll_timeout)
            except Queue.Empty:
                continue
            else:
                if self._dismissed.isSet():
                    self._requests_queue.put(request)
                    break
                self._requests_queue.task_done()
                try:
                    result = request.callable(**request.kwargs)
                    self._results_queue.put((request, result))
                except:
                    request.exception = True
                    self._results_queue.put((request, sys.exc_info()))

    def dismiss(self):
        """ 停止线程 """
        self._dismissed.set()

class WorkRequest:
    def __init__(self, _callable, _kwargs=None, _requestID=None, _callback=None):
        if _requestID is None:
            self.requestID = id(self)
        else:
            try:
                self.requestID = hash(requestID)
            except TypeError:
                raise TypeError("requestID must be hashable.")
        self.exception = False
        self.callback = _callback
        self.callable = _callable
        self.kwargs = _kwargs or {}
        #self.kwargs["_request"] = self

    def __str__(self):
        return "<WorkRequest id=%s kwargs=%r exception=%s>" % \
            (self.requestID, self.kwargs, self.exception)
        

class ThreadPool(object):
    DONE = 0
    WORKING = 1
    READY = 2
    """ 一个简单的线程池，使用Queue模块作为队列，任务封装在WorkRequest类中，\
线程对象是WorkThread """
    def __init__(self, _request_queue=None, _results_queue=None):
        super(ThreadPool, self).__init__()
        self.requests_queue = _request_queue or Queue.Queue(0)
        self.results_queue = _results_queue or Queue.Queue(0)
        self.workRequests = {}
        self.workers = []

    def create_workers(self, num_workers, poll_timeout=5):
        """ 创建工作组的线程 """
        for i in range(num_workers):
            self.workers.append(WorkerThread(self.requests_queue,
                self.results_queue, poll_timeout=poll_timeout))

    def stop_all_workers(self):
        """ 停止一切线程的工作,如果需要再次使用的时候,需要再次创建线程 """
        dis_list = []
        for worker in self.workers:
            worker.dismiss()
            dis_list.append(worker)

        for worker in dis_list:
            worker.join()

    def put_request(self, request):
        """ 封装好的WorkRequest 添加到队列中"""
        assert isinstance(request, WorkRequest)
        self.requests_queue.put_nowait(request)
        self.workRequests[request.requestID] = request
        self.state = ThreadPool.READY

    def poll(self):
        """ 线程池开始工作 """
        self.state = ThreadPool.WORKING 
        while(True):
            try:
                request, result = self.results_queue.get(timeout=10)
                if request.callback:
                    request.callback(request, result)
                del self.workRequests[request.requestID]
            except Queue.Empty:
                break
        self.state = ThreadPool.DONE

