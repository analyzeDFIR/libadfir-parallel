## -*- coding: UTF-8 -*-
## pool.py
##
## Copyright (c) 2018 analyzeDFIR
## 
## Permission is hereby granted, free of charge, to any person obtaining a copy
## of this software and associated documentation files (the "Software"), to deal
## in the Software without restriction, including without limitation the rights
## to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
## copies of the Software, and to permit persons to whom the Software is
## furnished to do so, subject to the following conditions:
## 
## The above copyright notice and this permission notice shall be included in all
## copies or substantial portions of the Software.
## 
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
## IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
## FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
## AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
## LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
## OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
## SOFTWARE.

from multiprocessing import JoinableQueue, cpu_count
CPU_COUNT = cpu_count()

from .utils import merge_kwargs

class WorkerPool(object):
    '''
    Class to manage pool of worker processes
    '''
    def __init__(self,
        queue,
        worker_class,
        worker_count=(2 if CPU_COUNT <= 4 else 4),
        daemonize=True,
        task_class=None,
        worker_kwargs=None,
        task_kwargs=None,
        config=None
    ):
        self.__queue = None
        self.__worker_class = None
        self.__worker_count = None
        self.__daemonize = None
        self.__task_class = None
        self.__worker_kwargs = None
        self.__task_kwargs = None
        self.__workers = None
        for attr, value in merge_kwargs(
            config, 
            worker_count=worker_count, 
            daemonize=daemonize,
            task_class=task_class,
            worker_kwargs=worker_kwargs,
            task_kwargs=task_kwargs
        ).items():
            setattr(self, attr, value)
    @property
    def queue(self):
        '''
        getter for queue
        '''
        return self.__queue
    @queue.setter
    def queue(self, value):
        '''
        setter for queue
        '''
        assert value is None or isinstance(value, JoinableQueue)
        self.__queue = value
    @property
    def worker_class(self):
        '''
        getter for worker_class
        '''
        return self.__worker_class
    @worker_class.setter
    def worker_class(self, value):
        '''
        setter for worker_class
        '''
        assert value is None or (callable(value) and issubclass(value, object))
        self.__worker_class = value
    @property
    def worker_count(self):
        '''
        getter for worker_count
        '''
        return self.__worker_count
    @worker_count.setter
    def worker_count(self, value):
        '''
        setter for worker_count
        '''
        assert value is None or isinstance(value, int)
        if value is None:
            self.__worker_count = 0
        elif value > 0 and value <= CPU_COUNT:
            self.__worker_count = value
        else:
            self.__worker_count = CPU_COUNT
    @property
    def daemonize(self):
        '''
        getter for daemonize
        '''
        return self.__daemonize
    @daemonize.setter
    def daemonize(self, value):
        '''
        setter for daemonize
        '''
        assert value is None or isinstance(value, bool)
        self.__daemonize = value if value is not None else False
    @property
    def task_class(self):
        '''
        getter for task_class
        '''
        return self.__task_class
    @task_class.setter
    def task_class(self, value):
        '''
        setter for task_class
        '''
        assert value is None or (callable(value) and issubclass(value, object))
        self.__task_class = value
    @property
    def worker_kwargs(self):
        '''
        getter for worker_kwargs
        '''
        return self.__worker_kwargs
    @worker_kwargs.setter
    def worker_kwargs(self, value):
        '''
        setter for worker_kwargs
        '''
        assert value is None or isinstance(value, dict)
        self.__worker_kwargs = value if value is not None else dict()
    @property
    def task_kwargs(self):
        '''
        getter for task_kwargs
        '''
        return self.__task_kwargs
    @task_kwargs.setter
    def task_kwargs(self, value):
        '''
        setter for task_kwargs
        '''
        assert value is None or isinstance(value, dict)
        self.__task_kwargs = value if value is not None else dict()
    @property
    def workers(self):
        '''
        getter for workers
        '''
        return self.__workers
    @workers.setter
    def workers(self, value):
        '''
        setter for workers
        '''
        assert value is None or isinstance(value, list)
        self.__workers = value
    def add_task(self, *args, task=None, poison_pill=False, **kwargs):
        '''
        Args:
            task: Object            => class to use to create new task
            poison_pill: Boolean    => add poison pill to queue instead fo new task
        Procedure:
            Add new task to queue:
                1) If poison_pill is True then poison pill is added to queue
                2) If task is not None then it is checked for being a callable 
                and is added to queue.
                3) If self.task_class is not None then self.task_class is used
                to add task to queue
                4) Otherwise raise Exception that no valid task constructor found.
            NOTE: *args and **kwargs will be merged with self.task_kwargs and pushed to
                  the task when it's created
        Preconditions:
            task is type subclass of Object (and thus callable)
            poison_pill is of type Boolean
        '''
        assert task is None or (callable(task) and issubclass(task, object))
        assert isinstance(poison_pill, bool)
        if self.queue is None:
            raise Exception('Must initialize a queue before attempting to add a task')
        if poison_pill:
            task = None
        if task is None or not callable(task):
            if self.task_class is not None:
                task = self.task_class(
                    *args,
                    **merge_kwargs(
                        kwargs,
                        **self.task_kwargs
                    )
                )
            else:
                raise Exception('No valid Task constructor provided')
        self.queue.put(task)
    def add_poison_pills(self):
        '''
        Args:
            N/A
        Procedure:
            Add poison pills to queue to kill the worker processes
        Preconditions:
            N/A
        '''
        if self.worker_count is None:
            raise Exception('Cannot add poison pills with worker count as None')
        for idx in range(self.worker_count):
            self.add_task(poison_pill=True)
    def initialize_workers(self):
        '''
        Args:
            N/A
        Procedure:
            Initialize self.worker_count number of workers of type self.worker_class.
            If self.worker_class is None or self.worker_count is None or
            self.workers isn't None then raise Exception
        Preconditions:
            self.worker_class takes at least the positional arguments queue and worker index    (assumed True)
        '''
        if self.worker_class is None or self.worker_count is None:
            raise Exception('Must set the worker class and worker count before initializing workers')
        elif self.workers is not None:
            raise Exception('Cannot initialize workers more than once')
        self.workers = [
            self.worker_class(self.queue, idx, **self.worker_kwargs)
            for idx in range(self.worker_count)
        ]
    def start_workers(self, initialize=True):
        '''
        Args:
            initialize: Boolean => initialize workers if not already initialized
        Procedure:
            Start all the workers in self.workers
        Preconditions:
            initialize is of type Boolean
        '''
        assert isinstance(initialize, bool)
        if self.workers is None:
            if not initialize:
                raise Exception('Must initialize workers or set the initialize argument to \'True\'')
            self.initialize_workers()
        for worker in self.workers:
            if not worker.is_alive():
                worker.daemon = self.daemonize
                worker.start()
    def join_workers(self):
        '''
        Args:
            N/A
        Procedure:
            Join on the living workers until all are finished
        Preconditions:
            N/A
        '''
        if self.workers is not None:
            for worker in self.workers:
                if worker.is_alive():
                    worker.join()
    def terminate_workers(self):
        '''
        Args:
            N/A
        Procedure:
            Terminate all living workers in self.workers
        Preconditions:
            N/A
        '''
        if self.workers is not None:
            for worker in self.workers:
                if worker.is_alive():
                    worker.terminate()
    def refresh_workers(self):
        '''
        Args:
            N/A
        Procedure:
            Terminate all living workers and re-initialize workers
        Preconditions:
            N/A
        '''
        if self.workers is not None:
            self.terminate_workers()
        self.workers = None
        self.initialize_workers()
    def join_tasks(self):
        '''
        Args:
            N/A
        Procedure:
            Join on self.queue until all the tasks are completed
        Preconditions:
            self.queue is of type JoinableQueue (assumed True and enforced in setter)
        '''
        if self.queue is not None:
            self.queue.join()
