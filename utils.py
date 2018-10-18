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
                for path in file_list:
                    remove(path)

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
        logging_config['handlers'] = [ProcessAwareFileHandler(filename, mode, encoding)]
        logging.basicConfig(**logging_config)
    else:
        for handler in logger.handlers:
            if isinstance(handler, ProcessAwareFileHandler):
                if handler.get_stream_config() is None:
                    handler.set_stream_config(dict(\
                        filename=filename, 
                        mode=mode, 
                        encoding=encoding\
                    ))

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
        self.set_filename(path.abspath(filename))
        self.set_mode(mode)
        self.set_encoding(encoding)
        if delay:
            self.stream = None
        else:
            self._open()
    def _getpid(self, pid=None):
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
    def _get_stream_attribute(self, attribute, pid):
        '''
        Args:
            @self._getpid
            attribute: String   => attribute of stream to receive
        Returns:
            Any
            stream attribute of pid's stream
        Preconditions:
            @self._getpid
            attribute is of type String
        '''
        assert isinstance(attribute, str), 'Attribute is not of type String'
        self.acquire()
        try:
            return self.streams.get(self._getpid(pid), dict()).get(attribute)
        finally:
            self.release()
    def _set_stream_attribute(self, attribute, value, pid):
        '''
        '''
        self.acquire()
        try:
            self.streams[self._getpid(pid)][attribute] = value
        finally:
            self.release()
    def get_filename(self, pid=None):
        '''
        '''
        return self._get_stream_attribute('filename', pid)
    def set_filename(self, filename, pid=None):
        '''
        '''
        assert filename is not None, 'Filename is not of type String'
        self._set_stream_attribute('filename', filename, pid)
    def get_mode(self, pid=None):
        '''
        '''
        return self._get_stream_attribute('mode', pid)
    def set_mode(self, mode='a', pid=None):
        '''
        '''
        self._set_stream_attribute('mode', mode, pid)
    def get_encoding(self, pid=None):
        '''
        '''
        return self._get_stream_attribute('encoding', pid)
    def set_encoding(self, encoding='UTF-8', pid=None):
        '''
        '''
        self._set_stream_attribute('encoding', encoding, pid)
    def get_stream(self, pid=None):
        '''
        '''
        return self._get_stream_attribute('stream', pid)
    def set_stream(self, stream=None, pid=None):
        '''
        '''
        current_PID = self._getpid() if pid is None else pid
        assert stream is not None or (\
            self.get_filename(current_PID) is not None and \
            self.get_mode(current_PID) is not None\
        ), 'Stream is not file-like object'
        if stream is None:
            self.set_stream(open(\
                self.get_filename(current_PID), 
                self.get_mode(current_PID), 
                encoding=self.get_encoding(current_PID)\
            ), current_PID)
        else:
            self._set_stream_attribute('stream', stream, pid)
    def get_stream_config(self, pid=None):
        '''
        '''
        return self.streams.get(self._getpid(pid), None)
    def set_stream_config(self, stream_config, pid=None):
        '''
        '''
        assert stream_config is not None, 'Stream_config is not dict-like object'
        if 'filename' in stream_config and 'mode' in stream_config:
            for attribute in ['filename', 'mode', 'encoding', 'stream']:
                if attribute in stream_config:
                    getattr(self, 'set_' + attribute)(stream_config.get(attribute), pid)
    def close(self):
        '''
        Closes all streams in self.streams
        '''
        self.acquire()
        try:
            try:
                exception = None
                for key in self.streams:
                    try:
                        self.get_stream(key).flush()
                    except Exception as e:
                        exception = e
                    finally:
                        current_stream = self.get_stream(key)
                        self.set_stream(None, key)
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
    def _open(self, pid=None):
        current_PID = self._getpid(pid)
        if current_PID in self.streams and \
                self.get_stream(current_PID) is None and \
                (self.get_filename(current_PID) is not None and\
                 self.get_mode(current_PID) is not None):
            self.set_stream(pid=current_PID)
    def emit(self, record):
        '''
        '''
        self._open()
        self.stream = self.get_stream()
        logging.StreamHandler.emit(self, record)
