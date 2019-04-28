## -*- coding: UTF-8 -*-
## pipeline.py
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

from typing import Dict, Any, Optional
from enum import Enum

from common.task import TaskResult, BaseTask

class ExecutionUnitType(Enum):
    '''
    Execution unit type enum.
    '''
    GENERATE    = 0
    CONSUME     = 1

class ExecutionUnit(BaseTask):
    '''
    Class representing a unit of execution in a pipeline.
    (see: Pipeline)
    '''
    def __init__(self, task_cls: BaseTask, task_config: Dict[str, Any]) -> ExecutionUnit:
        self.task_cls = task_cls
        self.task_config = task_config
        self.unit_type = None
    @property
    def task_cls(self) -> BaseTask:
        '''
        Getter for task_cls
        '''
        return self.__task_cls
    @task_cls.setter
    def task_cls(self, value: BaseTask):
        '''
        Setter for task_cls
        '''
        self.__task_cls = value
    @property
    def task_config(self) -> Dict[str, Any]:
        '''
        Getter for task_config
        '''
        return self.__task_config
    @task_config.setter
    def task_config(self, value: Dict[str, Any]):
        '''
        Setter for task_config
        '''
        self.__task_config = value
    @property
    def unit_type(self) -> Optional[ExecutionUnitType]:
        '''
        Getter for unit_type
        '''
        return self.__unit_type
    @unit_type.setter
    def unit_type(self, value: Optional[ExecutionUnitType]):
        '''
        Setter for unit_type
        '''
        self.__unit_type = value
    def _process_task(self) -> None:
        '''
        @BaseTask._process_task
        '''
        method = getattr(self, '_%s'%self.unit_type.name.lower())
        is method is None or not callable():
            raise NotImplementedError('%s unit type is not supported'%self.unit_type.name)
        method()

pipeline = Pipeline()\
    .register_task(ParseCommandLineArguments())\
    .register_task(GenerateMIDISequences())\
    .register_task(WriteMIDIFileToDisk())
pipeline.run()

pipeline = Pipeline() | \
    ParseCommandLineArguments() | \
    GenerateMIDISequences() | \
    WriteMIDIFileToDisk()
pipeline.run()

Task Types:
    Synchronous Execution Unit
        Synchronously (in main thread) runs task
        and passes result data directly to next task in pipeline. Cannot
        be used after an Asynchronous Pipeline Task.
    Asynchronous Pipeline Task
        Runs task in separate thread and immediately proceeds
        to the next task in the pipeline. Cannot be used before a Synchronous
        Pipeline Task that depends on the complete output of the task.
        For example, if an Asynchronous Pipeline Task writes N files to 
        disk, a Synchronous Pipeline Task that comes next in the pipeline
        mustnt rely upon all N files being present.  This is because the pipeline
        will kick off the Async and immediately start the Sync task, possibly
        before all N files are fully written to disk.

Pipeline Class:
    Attributes:
        tasks -> List[PipelineTask]
        queues -> List[JoinableQueue]
        pools -> List[WorkerPool]
        log_path -> String
    Methods:
        register_task => self.tasks.add
        run =>
            if self.has_async_jobs:
                for idx, job in enumerate(self.jobs[::-1]):
                    if isinstance(job, AsyncJob):
                        self.queues.insert(0,JoinableQueue())
                        self.pools.insert(0, WorkerPool(
                            self.queues[0],
                            LoggedWorker,
                            worker_count=job.worker_count,
                            task_class=job.task_class,
                            task_config=job.task_config
                        ))
                        if not idx == 0 and len(self.queues) > 1:
                            self.pools[0].worker_config = dict(result_queue=self.queues[1])






class GenerateExecutionUnit(BaseTask):

class TaskPipeline(BaseTask):
    '''
    Class for executing a pipeline of tasks, passing the TaskResult
    of each task to the next.  The TaskResult of the last task is
    set as the result of the TaskPipeline, allowing for composition
    of TaskPipelines.
    
    Task Pipeline consists of:
        1) N execution units (task type + configuration data)
        2) N-1 queues to pass results along the pipeline

    The first execution unit in the pipeline takes program input 
    from a non-pipline object, typically command line arguments
    passed by a user.  Every other task takes a TaskResult object, as well
    as any configuration data, as arguments.
    '''
    def __init__(self, tasks, first_config=None):
        super().__init__()
        self.tasks = tasks
        self.first_config = first_config
    def __call__(self, **kwargs):
        if len(kwargs) > 0:
            if self.first_config is None:
                self.first_config = kwargs
            else:
                self.first_config.update(kwargs)
        super().__call__()
    @property
    def tasks(self):
        '''
        Getter for tasks
        '''
        return self.__tasks
    @tasks.setter
    def tasks(self, value):
        '''
        Setter for tasks
        '''
        assert isinstance(value, list) and \
            len(value) > 0 and \
            all([
                isinstance(task, BaseTask) \
                for task in value
            ])
        self.__tasks = value
    @property
    def first_config(self):
        '''
        Getter for first_config
        '''
        return self.__first_config
    @first_config.setter
    def first_config(self, value):
        '''
        Setter for first_config
        '''
        assert value is None or isinstance(value, dict)
        self.__first_config = value
    def _process_task(self):
        '''
        @BaseTask._process_task
        '''
        config = self.first_config
        for task in self.tasks:
            if isinstance(task_cls, TaskPipeline):
                result = task(**config)
            else:
                result = task(**config)()
            config = result.state.get('next_config')
        self.result = result

