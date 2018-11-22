## -*- coding: UTF-8 -*-
## utils.py
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
from os import path, rename, remove, getpid
from io import TextIOWrapper
from collections import defaultdict
from glob import glob
from heapq import merge as heapq_merge

def merge_kwargs(config, **kwargs):
    '''
    Args:
        config: Dict<String, Any>   => configuration to apply to object
    Returns:
        Dict<String, Any>
        For each keyword argument in kwargs, check if that argument exists in
        config and, if so, bind the config value to the instance.  Otherwise,
        bind the keyword argument value to the instance.  Finally, apply any key
        in config that wasn't already applied.
    Preconditions:
        config is of type Dict<String, Any> (assumed True)
    '''
    result = dict()
    for kwarg in kwargs:
        if config is not None and kwarg in config:
            result[kwarg] = config.get(kwarg)
            del config[kwarg]
        else:
            result[kwarg] = kwargs.get(kwarg)
    if config is not None and len(config) > 0:
        for kwarg in config:
            result[kwarg] = config.get(kwarg)
    return result

def coalesce_files(glob_pattern, target, transform=lambda line: line, clean=True):
    '''
    Args:
        glob_pattern: String                    => glob pattern of files to merge
        target: String                          => file path to merge files into
        transform: Callable<String> -> String   => transform to perform on each line
    Procedure:
        Gather all files that match glob_pattern and merge them into target
        NOTE: assumes each file is sorted and can be naturally sorted by columns
    Preconditions:
        glob_pattern is of type String
        target is of type String
        transform is of type Callable<String> -> String
    '''
    assert isinstance(glob_pattern, str)
    assert isinstance(target, str)
    assert callable(transform)
    file_list = glob(glob_pattern)
    if len(file_list) == 0:
        return
    elif len(file_list) == 1 and not path.exists(file_list[0]):
        rename(file_list[0], target)
    else:
        handle_list = [open(filepath, 'r') for filepath in file_list]
        merged_records = heapq_merge(*[(transform(line) for line in handle) for handle in handle_list])
        try:
            with open(target, 'a') as target_file:
                for record in merged_records:
                    target_file.write(record)
        finally:
            for handle in handle_list:
                handle.close()
            if clean:
                for fpath in file_list:
                    remove(fpath)

def closeFileHandlers(logger=logging.root):
    '''
    Args:
        logger: logging.Logger  => logger to add handler to
    Procedure:
        Close all handlers attached to logger pointing to files (handlers that subclass FileHandler)
    Preconditions:
        logger is subclass of logging.Logger    (assumed True)
    '''
    for handler in logger.handlers:
        if issubclass(type(handler), logging.FileHandler):
            if hasattr(handler, 'close') and callable(handler.close):
                handler.close()
                logger.removeHandler(handler)

def addProcessScopedHandler(filename, logger=logging.root, mode='a', encoding='UTF-8', logging_config=None):
    '''
    Args:
        filename: String                    => filename to add handler for
        logger: logging.Logger              => logger to add handler to
        mode: String                        => file mode for new handler
        encoding: String                    => encoding to open new file with
        logging_config: Dict<String, Any>   => config to apply to root logger if
                                               if len(logger.handlers) == 0
    Procedure:
        Adds new stream to first ProcessAwareFileHandler encountered in logger.handlers
        if length of logger.handlers > 0, else create a new handler 
    Preconditions:
        filename is of type String
        logger is subclass of logging.Logger        (assumed True)
        mode is of type String
        encoding is of type String
        logging_config is of type Dict<String, Any>
    '''
    assert isinstance(filename, str)
    assert isinstance(mode, str)
    assert isinstance(encoding, str)
    assert logging_config is None or isinstance(logging_config, dict)
    if logger.handlers is None or len(logger.handlers) == 0:
        if logging_config is None:
            logging_config = dict()
        logging_config['handlers'] = [ProcessAwareFileHandler(filename, mode, encoding)]
        logging.basicConfig(**logging_config)
    else:
        for handler in logger.handlers:
            if isinstance(handler, ProcessAwareFileHandler):
                if handler.stream_config is None:
                    handler.stream_config = dict(
                        filename=filename, 
                        mode=mode, 
                        encoding=encoding
                    )

class ProcessAwareFileHandler(logging.FileHandler):
    '''
    A handler class which writes formatted logging records to disk files,
    but that can be configured to write to different files based on PID
    '''

    def __init__(self, filename, mode='a', encoding='UTF-8', delay=False):
        #keep the absolute path, otherwise derived classes which use this
        #may come a cropper when the current directory changes
        filename = path.abspath(filename)
        logging.Handler.__init__(self)
        self.streams = defaultdict(dict)
        self.filename = path.abspath(filename)
        self.mode = mode
        self.encoding = encoding
        if delay:
            self.stream = None
        else:
            self._open()
    def __getpid(self, pid=None):
        '''
        Args:
            pid: Integer|String => process identifier (PID)
        Returns:
            String
            Current PID
        Preconditions:
            pid is of type Integer or String
        '''
        assert isinstance(pid, (int, str, type(None))), 'PID is not of type Integer or String'
        return str(getpid()) if pid is None else str(pid)
    def __get_attribute(self, attribute):
        '''
        Args:
            attribute: String   => attribute of stream to receive
        Returns:
            Any
            stream attribute of pid's stream
        Preconditions:
            attribute is of type String
        '''
        assert isinstance(attribute, str)
        self.acquire()
        try:
            return self.streams[self.__getpid()].get(attribute)
        finally:
            self.release()
    def __set_attribute(self, attribute, value):
        '''
        Args:
            attribute: String   => attribute of stream to receive
            value: Any          => value to set attribute to
        Procedure:
            Set stream attribute to value
        Preconditions:
            attribute is of type String
        '''
        assert isinstance(attribute, str)
        self.acquire()
        try:
            self.streams[self.__getpid()][attribute] = value
        finally:
            self.release()
    @property
    def filename(self):
        '''
        Getter for filename
        '''
        return self.__get_attribute('filename')
    @filename.setter
    def filename(self, value):
        '''
        Setter for filename
        '''
        assert isinstance(value, str)
        self.__set_attribute('filename', value)
    @property
    def mode(self):
        '''
        Getter for mode
        '''
        return self.__get_attribute('mode')
    @mode.setter
    def mode(self, value):
        '''
        Setter for mode
        '''
        assert isinstance(value, str)
        self.__set_attribute('mode', value)
    @property
    def encoding(self):
        '''
        Getter for encoding
        '''
        return self.__get_attribute('encoding')
    @encoding.setter
    def encoding(self, value):
        '''
        Setter for encoding
        '''
        assert isinstance(value, str)
        self.__set_attribute('encoding', value)
    @property
    def stream(self):
        '''
        Getter for stream
        '''
        return self.__get_attribute('stream')
    @stream.setter
    def stream(self, value):
        '''
        Setter for stream
        '''
        assert isinstance(value, (TextIOWrapper, type(None)))
        self.__set_attribute('stream', value)
    @property
    def stream_config(self):
        '''
        Getter for stream_config
        '''
        return self.streams.get(self.__getpid())
    @stream_config.setter
    def stream_config(self, value):
        '''
        Setter for stream_config
        '''
        assert isinstance(value, dict)
        assert 'filename' in value and 'mode' in value
        for attr in ['filename', 'mode', 'encoding', 'stream']:
            if attr in value:
                setattr(self, attr, value.get(attr))
    def create_stream(self):
        '''
        Args:
            N/A
        Returns:
            File-like object
            Creates stream using the current filename, mode, and encoding
        Preconditions:
            self.filename is not None
            self.mode is not None
            self.encoding is not None
        '''
        assert self.filename is not None \
            and self.mode is not None \
            and self.encoding is not None
        return open(self.filename, self.mode, encoding=self.encoding)
    def close(self):
        '''
        Closes all streams in self.streams
        '''
        self.acquire()
        try:
            try:
                exception = None
                for pid in self.streams:
                    try:
                        stream = self.streams.get(pid).get('stream')
                        if hasattr(stream, 'flush'):
                            stream.flush()
                    except Exception as e:
                        exception = e
                    finally:
                        current_stream = self.stream
                        self.stream = None
                        if hasattr(current_stream, 'close'):
                            current_stream.close()
                if exception is not None:
                    raise exception
            finally:
                # Issue #19523: call unconditionally to
                # prevent a handler leak when delay is set
                logging.StreamHandler.close(self)
        finally:
            self.release()
    def _open(self):
        self.stream = self.create_stream()
    def emit(self, record):
        '''
        @logging.FileHandler
        '''
        self._open()
        logging.StreamHandler.emit(self, record)
