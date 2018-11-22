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
from multiprocessing.queues import JoinableQueue as JoinableQueueType

from .task import TaskResult, BaseTask
from .utils import addProcessScopedHandler

class BaseWorker(BaseTask, Process):
    '''
    Base worker class for running tasks 
    from a (potentially shared) queue
    '''
    def __init__(self, source_queue, idx, target_queue=None, name=lambda: str(uuid4()), **context):
        Process.__init__(self, name=(name() if callable(name) else name))
        BaseTask.__init__(self)
        self.source_queue = source_queue
        self.idx = idx
        self.target_queue = target_queue
        self.context = context
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
        assert value is None or isinstance(value, JoinableQueueType)
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
        assert value is None or isinstance(value, JoinableQueueType)
        self.__target_queue = value
    @property
    def context(self):
        '''
        Getter for context
        '''
        return self.__context
    @context.setter
    def context(self, value):
        '''
        Setter for context
        '''
        assert value is None or isinstance(value, dict)
        self.__context = value
    def _result_callback(self):
        '''
        Args:
            N/A
        Procedure:
            Callback when a task is completed and no exception is thrown.
            By default it checks if the current task produced new tasks
            to be added to the target_queue and adds them if any are found.
        Preconditions:
            N/A
        '''
        if self.target_queue is not None and \
            self.result.state is not None and \
            'next_tasks' in self.result.state:
            for next_task in self.result.state.get('next_tasks'):
                self.target_queue.put(next_task)
    def _error_callback(self):
        '''
        Args:
            N/A
        Procedure:
            Callback when an exception as thrown trying to
            complete a task
        Preconditions:
            N/A
        '''
        pass
    def _process_task(self):
        '''
        Args:
            @BaseTask.__proces_task
        Procedure:
            @BaseTask.__process_task
        Preconditions:
            @BaseTask.__proces_task
            Task from queue is subclass of BaseTask or callable
            that returns TaskResult
        '''
        while True:
            try:
                task = self.source_queue.get()
                if task is None:
                    self.result = TaskResult(dict(proceed=False))
                else:
                    self.result = task(self.context) if callable(task) else task
                self._result_callback()
            except Exception as e:
                self.result = TaskResult(dict(proceed=True, err=e))
                self._error_callback()
            finally:
                self.source_queue.task_done()
            if 'proceed' in self.result.state \
                and not self.result.state.get('proceed'):
                break

class LoggedWorker(BaseWorker):
    '''
    Worker class that logs at start and finish as well as
    if it encounters an exception processing a task
    '''
    def __init__(self, *args, log_path=None, temp_log=False, logging_config=None, **context):
        super().__init__(*args, **context)
        if temp_log:
            self.log_path = path.join(log_path, '%s_tmp.log'%self.name)
        else:
            self.log_path = log_path
        self.logging_config = logging_config
    @property
    def log_path(self):
        '''
        Getter for log_path
        '''
        return self.__log_path
    @log_path.setter
    def log_path(self, value):
        '''
        Setter for log_path
        '''
        assert isinstance(value, str)
        self.__log_path = value
    @property
    def logging_config(self):
        '''
        Getter for logging_config
        '''
        return self.__logging_config
    @logging_config.setter
    def logging_config(self, value):
        '''
        Setter for logging_config
        '''
        assert value is None or isinstance(value, dict)
        self.__logging_config = value
    def _preamble(self):
        '''
        @BaseWorker._preamble
        '''
        addProcessScopedHandler(self.log_path, logging_config=self.logging_config)
        Logger.info('Started worker: %s'%self.name)
    def _error_callback(self):
        '''
        @BaseWorker._preamble
        '''
        super()._error_callback()
        Logger.error('Failed to process task (%s)'%str(self.result.state.get('err')))
    def _postamble(self):
        '''
        @BaseWorker._postamble
        '''
        Logger.info('Ended worker: %s'%self.name)

try:
    from tqdm import tqdm
    class ProgressTrackedWorker(LoggedWorker):
        '''
        Worker class that tracks progress of the tasks
        completed by a number of workers by updating a
        shared tqdm instance
        '''
        def __init__(
            self, 
            *args, 
            progress_count=None, 
            progress_desc=None, 
            progress_unit=None, 
            progress_position=None,
            **context
        ):
            super().__init__(*args, **context)
            self.progress_count = progress_count
            self.progress_desc = progress_desc
            self.progress_unit = progress_unit
            self.progress_position = progress_position
        @property
        def progress_count(self):
            '''
            Getter for progress_count
            '''
            return self.__progress_count
        @progress_count.setter
        def progress_count(self, value):
            '''
            Setter for progress_count
            '''
            assert isinstance(value, int)
            self.__progress_count = value
        @property
        def progress_desc(self):
            '''
            Getter for progress_desc
            '''
            return self.__progress_desc
        @progress_desc.setter
        def progress_desc(self, value):
            '''
            Setter for progress_desc
            '''
            assert isinstance(value, str)
            self.__progress_desc = value
        @property
        def progress_unit(self):
            '''
            Getter for progress_unit
            '''
            return self.__progress_unit
        @progress_unit.setter
        def progress_unit(self, value):
            '''
            Setter for progress_unit
            '''
            assert isinstance(value, str)
            self.__progress_unit = value
        @property
        def progress_position(self):
            '''
            Getter for progress_position
            '''
            return self.__progress_position
        @progress_position.setter
        def progress_position(self, value):
            '''
            Setter for progress_position
            '''
            assert value is None or isinstance(value, str)
            self.__progress_position = value
        @property
        def progress(self):
            '''
            Getter for progress
            '''
            return self.__progress
        @progress.setter
        def progress(self, value):
            '''
            Setter for progress
            '''
            assert isinstance(value, tqdm)
            self.__progress = value
        def _preamble(self):
            '''
            @LoggedWorker._preamble
            '''
            super()._preamble()
            self.progress = tqdm(
                total=self.progress_count, 
                desc=self.progress_desc, 
                unit=self.progress_unit,
                position=self.progress_position
            )
        def _result_callback(self):
            '''
            @LoggedWorker.__result_callback
            '''
            super()._result_callback()
            self.progress.update(1)
        def _postamble(self):
            '''
            @LoggedWorker._postamble
            '''
            super()._postamble()
            self.progress.close()
except ImportError:
    Logger.warning('Failed to import tqdm, some worker classes will be unavailable')
