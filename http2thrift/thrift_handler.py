from __future__ import (unicode_literals, print_function, division, absolute_import)

import os
import fnmatch
from collections import namedtuple
import threading
from typing import Any, Dict, AnyStr

import thriftpy
import thriftpy.parser
from thriftpy.parser.exc import ThriftParserError
from thriftpy.rpc import make_client, TClient
from thriftpy.transport import TFramedTransportFactory

from http2thrift import get_logger
from http2thrift.call import call_method_wrapped


L = get_logger(__name__)


BaseRequest = namedtuple('Request', [
    'host', 'port', 'thrift_file', 'service', 'method', 'args',
])


class ResourceNotFound(Exception):
    pass


class ThriftRequest(BaseRequest):
    pass


def glob_recursive(dir, pattern):
    for root, dirnames, filenames in os.walk(dir, followlinks=True):
        for filename in fnmatch.filter(filenames, pattern):
            yield os.path.join(root, filename)


class FileMonitor(object):
    def __init__(self, dir, pattern):
        # type: (AnyStr, AnyStr) -> None
        self.dir = dir
        self.pattern = pattern

        # is file changed after last access
        self.path2status = dict()   # type: Dict[AnyStr, bool]

    # public
    def start(self):
        threading.Thread(target=self.collector_thread).start()
        # TODO: watchdog

    def list(self):
        return self.path2status.iterkeys()

    def contains(self, path):
        exist = os.path.normpath(path) in self.path2status
        if not exist:
            full = os.path.join(self.dir, path)
            if fnmatch.fnmatch(full, self.pattern) and os.path.exists(full):
                self.add(path)
                assert self.contains(path)
                exist = True
        return exist

    def access(self, path):
        self.path2status[os.path.normpath(path)] = False

    def is_changed(self, path):
        return self.path2status[os.path.normpath(path)]

    # private
    def collector_thread(self):
        L.debug('starting collector')
        for path in glob_recursive(self.dir, self.pattern):
            self.add(path)

    def add(self, path):
        # TODO: lock
        self.path2status[os.path.normpath(path)] = True


class ThriftHandler(object):
    # public
    def __init__(self, dir):
        self.dir = dir
        self.monitor = FileMonitor(dir, '*.thrift')
        # cached thrift module
        self.path2thrift = dict()

    def start(self):
        self.monitor.start()

    def call(self, req):
        # type: (ThriftRequest) -> dict
        service = self.get_service(req.thrift_file, req.service)
        client = self.get_client(service, req.host, req.port)

        return call_method_wrapped(service, client, req.method, req.args)

    def list_services(self, path=None):
        return list(self.list_modules_info(path))

    # private
    def list_modules_info(self, path=None):
        # type: () -> dict
        if path is None:
            pathlist = self.monitor.list()
        else:
            pathlist = [path]

        for path in pathlist:
            try:
                module = self.get_thrift_module(path)
            except ThriftParserError:
                L.exception('bad thrift file: "%s"', path)
                continue

            services = module.__thrift_meta__['services']
            services_info = {
                svc.__name__: list(self.list_methods_info(svc))
                for svc in services
            }
            yield dict(path=path, services=services_info)

    def list_methods_info(self, service):
        for method_name in service.thrift_services:
            yield dict(method=method_name)

    def get_client(self, service, host, port):
        # type: (Any, str, int) -> TClient
        # TODO: re-use client
        return make_client(service, host=host, port=port, trans_factory=TFramedTransportFactory())

    def get_thrift_module(self, path):
        # TODO: reload .thrift file
        if not self.monitor.contains(path):
            raise ResourceNotFound('thrift file not found: "%s"' % path)

        if path not in self.path2thrift or self.monitor.is_changed(path):
            fullpath = os.path.join(self.dir, path)
            L.debug('loading thrift file: %s', fullpath)
            module = thriftpy.parser.parse(str(fullpath), enable_cache=False)   # path must be str in py2
            self.path2thrift[path] = module
            self.monitor.access(path)

        return self.path2thrift[path]

    def get_service(self, thrift_file, service_name):
        module = self.get_thrift_module(thrift_file)
        if service_name == '*':
            services = module.__thrift_meta__['services']
            if len(services) == 1:
                return services[0]

        try:
            return getattr(module, service_name)
        except AttributeError:
            raise ResourceNotFound('service "%s" not found in "%s"' % (service_name, thrift_file))


_handler = None

def get_handler():
    global _handler
    if _handler is None:
        _handler = ThriftHandler('.')
        _handler.start()

    return _handler
