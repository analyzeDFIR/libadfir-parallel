## -*- coding: UTF-8 -*-
## worker.py
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

import logging
Logger = logging.getLogger(__name__)
from os import path
from uuid import uuid4
from multiprocessing import Process, JoinableQueue

from .utils import addProcessScopedHandler

class TaskResult(object):
    '''
    Class used to hold the resulting state of processing
    a task from a queue
    '''
    def __init__(self, proceed=True, state=None):
        self.proceed = proceed
        self.state = state
    @property
    def proceed(self):
        '''
        Getter for proceed
        '''
        return self.__proceed
    @proceed.setter
    def proceed(self, value):
        '''
        Setter for proceed
        '''
        assert isinstance(value, bool)
        self.__proceed = value if value is not None else False
    @property
    def state(self):
        '''
        Getter for state
        '''
        return self.__state
    @state.setter
    def state(self, value):
        '''
        Setter for state
        '''
        assert value is None or isinstance(value, dict)
        self.__state = value

class BaseWorker(Process):
    '''
    Base worker class for running tasks 
    from a (potentially shared) queue
    '''
    def __init__(self, source_queue, idx, target_queue=None, name=lambda: str(uuid4())):
        super().__init__(name=name() if callable(name) else name)
        self.source_queue = source_queue,
        self.idx = idx
        self.target_queue = target_queue
    @property
    def source_queue(self):
        '''
        Getter for source_queue
        '''
        return self.__source_queue
    @source_queue.setter
    def source_queue(self, value):
        '''
        Setter for source_queue
        '''
        assert value is None or isinstance(value, JoinableQueue)
        self.__source_queue = value
    @property
    def idx(self):
        '''
        Getter for idx
        '''
        return self.__idx
    @idx.setter
    def idx(self, value):
        '''
        Setter for idx
        '''
        assert value is None or isinstance(value, int)
        self.__idx = value
    @property
    def target_queue(self):
        '''
        Getter for target_queue
        '''
        return self.__target_queue
    @target_queue.setter
    def target_queue(self, value):
        '''
        Setter for target_queue
        '''
        assert value is None or isinstance(value, JoinableQueue)
        self.__target_queue = value
    def _preamble(self):
        '''
        Args:
            N/A
        Procedure:
            Perform worker initialization tasks
        Preconditions:
            N/A
        '''
        pass
    def _result_callback(self, task_result):
        '''
        Args:
            task_result: TaskResult => result of last task processed
        Procedure:
            Callback when a task is completed and no exception is thrown.
            By default it checks if the current task produced new tasks
            to be added to the result_queue and adds them if any are found.
        Preconditions:
            task_result is of type TaskResult
        '''
        assert isinstance(task_result, TaskResult)
        if self.result_queue is not None and \
            task_result.state is not None and \
            'next_tasks' in task_result.state:
            for next_task in task_result.state.get('next_tasks'):
                self.result_queue.put(next_task)
    def _error_callback(self, task_result):
        '''
        Args:
            task_result: TaskResult => result of last task processed
        Procedure:
            Callback when an exception as thrown trying to
            complete a task
        Preconditions:
            task_result is of type TaskResult
        '''
        assert isinstance(task_result, TaskResult)
    def _process_task(self):
        '''
        Args:
            N/A
        Returns:
            TaskResult
            Whether the worker should continue and the resulting
            state of running the task
        Preconditions:
            Tasks are either of type Dict<String, Any> or any callable
            that, when called, produces a Dict<String, Any>.
        '''
        task = self._queue.get()
        task_result = TaskResult()
        if task is None:
            task_result.proceed = False
        else:
            task_result.proceed = True
            task_result.state = task() if callable(task) else task
        return task_result
    def _postamble(self):
        '''
        Args:
            N/A
        Procedure:
            Performs worker teardown tasks
        Preconditions:
            N/A
        '''
        return None
    def run(self):
        '''
        Args:
            N/A
        Procedure:
            Run the worker, picking tasks off the queue until 
            a poison pill (None) is encountered
        Preconditions:
            N/A
        '''
        self._preamble()
        while True:
            try:
                task_result = self._process_task()
            except Exception as e:
                self._error_callback(TaskResult(
                    True,
                    dict(err=e)
                ))
            else:
                self._result_callback(task_result)
                if not task_result.proceed:
                    break
        self._postamble()

class LoggedWorker(BaseWorker):
    '''
    Worker class that logs at start and finish as well as
    if it encounters an exception processing a task
    '''
    def __init__(*args, log_path, temp_log=False, **kwargs):
        super().__init__(*args, **kwargs)
        if temp_log:
            self.log_path = path.join(log_path, '%s_tmp.log'%self.name)
        else:
            self.log_path = log_path
    @property
    def log_path(self):
        '''
        Getter for log_path
        '''
        return self._log_path
    @log_path.setter
    def log_path(self, value):
        '''
        Setter for log_path
        '''
        assert isinstance(value, str)
        self._log_path = value
    def _preamble(self):
        '''
        @BaseWorker._preamble
        '''
        addProcessScopedHandler(self.log_path)
        Logger.info('Started worker: %s'%self.name)
    def _error_callback(self, task_result):
        '''
        @BaseWorker._preamble
        '''
        super()._error_callback(task_result)
        Logger.error('Failed to process task (%s)'%str(task_result.state.get('err')))
    def _postamble(self):
        '''
        @BaseWorker._postamble
        '''
        Logger.info('Ended worker: %s'%self.name)
