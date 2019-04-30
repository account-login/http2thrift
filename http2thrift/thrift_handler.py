from __future__ import (unicode_literals, print_function, division, absolute_import)

import os
import fnmatch
import traceback
from collections import namedtuple, OrderedDict, defaultdict
import threading
from typing import Any, Dict

import thriftpy
import thriftpy.parser
from thriftpy.thrift import TApplicationException, TException
from thriftpy.rpc import make_client, TClient
from thriftpy.transport import TFramedTransportFactory

from http2thrift import get_logger
from http2thrift.thrift_util import generate_sample_struct, get_args_obj, get_result_obj, struct_to_json


L = get_logger(__name__)


BaseRequest = namedtuple('Request', [
    'host', 'port', 'thrift_file', 'service', 'method', 'args',
])


class ResourceNotFound(Exception):
    pass


class MultipleMatchingService(Exception):
    pass


class ThriftRequest(BaseRequest):
    pass


def glob_recursive(dirpath, pattern):
    for root, dirnames, filenames in os.walk(dirpath, followlinks=True):
        for filename in fnmatch.filter(filenames, pattern):
            yield os.path.join(root, filename)


def handle_exception(exc, result):
    for k in sorted(result.thrift_spec):
        if result.thrift_spec[k][1] == "success":
            continue

        _, exc_name, exc_cls, _ = result.thrift_spec[k]
        if isinstance(exc, exc_cls):
            setattr(result, exc_name, exc)
            return True
    else:
        return False


def call_method(service, handler, method, args, result):
    try:
        f = getattr(handler, method)
    except AttributeError:
        raise TApplicationException(
            TApplicationException.INTERNAL_ERROR, 'method not implemented: %s' % (method,))

    args_list = [getattr(args, args.thrift_spec[k][1]) for k in sorted(args.thrift_spec)]

    try:
        result.success = f(*args_list)
    except Exception as exc:
        if not handle_exception(exc, result):
            raise


def call_method_with_dict(service, handler, method, args_dict):
    if method not in service.thrift_services:
        raise TApplicationException(
            TApplicationException.UNKNOWN_METHOD,
            'method "%s" not found in %r' % (method, service))

    args = get_args_obj(service, method, args_dict)
    result = get_result_obj(service, method)

    call_method(service, handler, method, args, result)
    return result


def wrap_exception(exc):
    return OrderedDict([
        ('exception_name', type(exc).__name__),
        ('exception', struct_to_json(exc))
    ])


def call_method_wrapped(service, handler, method, args_dict):
    # type: (Any, Any, str, dict) -> dict

    exception = None
    try:
        res = call_method_with_dict(service, handler, method, args_dict)
    except TException as texc:
        traceback.print_exc()
        exception = texc
    except Exception as exc:
        traceback.print_exc()
        exception = TApplicationException(
            TApplicationException.INTERNAL_ERROR, 'uncaught exception: %r' % exc)
    else:
        return struct_to_json(res)

    # exception
    assert exception is not None
    return wrap_exception(exception)


class ThriftModuleInfo(object):
    def __init__(self, path, module):
        self.path = path
        self.module = module


def _thrift_module_list_services(thrift_module):
    return thrift_module.__thrift_meta__['services']


def _thrift_service_name(thrift_svc):
    return thrift_svc.__name__


def _thrift_service_list_method(thrift_svc):
    return thrift_svc.thrift_services


def _thrift_parse_module(thrift_file):
    return thriftpy.parser.parse(str(thrift_file), enable_cache=False)  # path must be str in py2


class ThriftIndexer(object):
    def __init__(self, dirpath='.'):
        self.dir = dirpath
        self.lock = threading.Lock()
        # indexes
        self.path_to_module_info = dict()   # type: Dict[str, ThriftModuleInfo]
        self.service_to_module_info_set = defaultdict(set)
        self.method_to_module_info_set = defaultdict(set)

    def add(self, path, thrift_module=None):
        normpath = os.path.normpath(path)
        fullpath = os.path.join(self.dir, path)
        L.debug('loading thrift file: "%s"', fullpath)

        if thrift_module is None:
            try:
                thrift_module = _thrift_parse_module(fullpath)
            except Exception as exc:
                L.error('bad thrift file: "%s", exc: %r', path, exc)
                return
        mi = ThriftModuleInfo(normpath, thrift_module)

        # indexes
        with self.lock:
            self.path_to_module_info[normpath] = mi
            for thrift_svc in _thrift_module_list_services(thrift_module):
                self.service_to_module_info_set[_thrift_service_name(thrift_svc)].add(mi)
                for method in _thrift_service_list_method(thrift_svc):
                    self.method_to_module_info_set[method].add(mi)

    def query(self, path=None, service=None, method=None):
        svc_list = []

        with self.lock:
            if path is not None:
                normpath = os.path.normpath(path)
                if normpath in self.path_to_module_info:
                    svc_list = _thrift_module_list_services(self.path_to_module_info[normpath].module)

            if service is not None:
                if svc_list:
                    # filter by service
                    svc_list = [svc for svc in svc_list if _thrift_service_name(svc) == service]
                else:
                    # get service by service name
                    svc_list = [
                        svc
                        for mi in self.service_to_module_info_set[service]
                        for svc in _thrift_module_list_services(mi.module)
                        if _thrift_service_name(svc) == service
                    ]

            if method is not None:
                if svc_list:
                    # filter by method
                    svc_list = [svc for svc in svc_list if method in _thrift_service_list_method(svc)]
                else:
                    # get service by method name
                    svc_list = [
                        svc
                        for mi in self.method_to_module_info_set[method]
                        for svc in _thrift_module_list_services(mi.module)
                        if method in _thrift_service_list_method(svc)
                    ]

        return svc_list

    def list_path(self):
        return list(self.path_to_module_info.keys())


class ThriftHandler(object):
    # public
    def __init__(self, dirpath):
        self.dir = dirpath
        self.index = ThriftIndexer(dirpath)
        self.key2client = threading.local()

    def start(self):
        threading.Thread(target=self._collector_thread).start()

    # private
    def _collector_thread(self):
        L.debug('starting collector')
        for path in glob_recursive(self.dir, '*.thrift'):
            self.index.add(path)

    def call(self, req):
        # type: (ThriftRequest) -> dict
        service = self.get_service(req.thrift_file, req.service, req.method)
        try:
            client = self.get_client(service, req.host, req.port)
        except TException as texc:  # TTransportException and etc
            return wrap_exception(texc)

        # FIXME: retry send error
        rv = call_method_wrapped(service, client, req.method, req.args)
        if 'exception' in rv:
            self.drop_client(service, req.host, req.port, client)
        return rv

    def list_services(self, path=None):
        return list(self.list_modules_info(path))

    # TODO: search service by kw

    def get_sample(self, thrift_file, service_name, method):
        service = self.get_service(thrift_file, service_name, method)

        args = get_args_obj(service, method, dict())
        result = get_result_obj(service, method)
        generate_sample_struct(args, type(args))
        generate_sample_struct(result, type(result))
        return OrderedDict([
            ('args', struct_to_json(args)),
            ('result', struct_to_json(result))
        ])

    # private
    def list_modules_info(self, path=None):
        # type: () -> dict
        if path is None:
            pathlist = self.index.list_path()
        else:
            pathlist = [path]

        for path in pathlist:
            svc_list = self.index.query(path=path)

            services_info = {
                svc.__name__: list(self.list_methods_info(svc))
                for svc in svc_list
            }
            yield OrderedDict([
                ('path', path),
                ('services', services_info)
            ])

    def list_methods_info(self, service):
        for method_name in _thrift_service_list_method(service):
            yield dict(method=method_name)

    def get_client(self, service, host, port):
        # type: (Any, str, int) -> TClient

        if not hasattr(self.key2client, 'd'):
            self.key2client.d = dict()
        d = self.key2client.d

        key = service, host, port
        if key not in d:
            d[key] = make_client(service, host=host, port=port, trans_factory=TFramedTransportFactory())
        return d[key]

    def drop_client(self, service, host, port, client):
        L.debug('drop client')
        try:
            client.close()
        finally:
            self.key2client.d.pop((service, host, port))

    def get_service(self, thrift_file_pattern, service_pattern, method):
        path = None
        if thrift_file_pattern != '*':
            path = thrift_file_pattern
        service = None
        if service_pattern != '*':
            service = service_pattern

        svc_list = self.index.query(path=path, service=service, method=method)
        if not svc_list:
            raise ResourceNotFound('pattern {!r} not found'.format((thrift_file_pattern, service_pattern, method)))

        if len(svc_list) > 1:
            raise MultipleMatchingService('multiple services: {!r}'.format(list(map(_thrift_service_name, svc_list))))

        return svc_list[0]


_handler = None


def get_handler():
    global _handler
    if _handler is None:
        # TODO: supports multiple path
        dirpath = os.environ.get('HTTP2THRIFT_PATH', '.')
        _handler = ThriftHandler(dirpath)
        _handler.start()

    return _handler
