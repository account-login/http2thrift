from __future__ import (unicode_literals, print_function, division, absolute_import)

from collections import namedtuple

import thriftpy
from thriftpy.rpc import make_client, TClient
from thriftpy.transport import TFramedTransportFactory

from http2thrift.call import call_method_wrapped


BaseRequest = namedtuple('Request', [
    'host', 'port', 'thrift_file', 'service', 'method', 'args',
])


class ResourceNotFound(Exception):
    pass


class ThriftRequest(BaseRequest):
    pass


class ThriftHandler(object):
    def call(self, req):
        # type: (ThriftRequest) -> dict
        service = self.get_service(req.thrift_file, req.service)
        client = self.get_client(service, req.host, req.port)

        return call_method_wrapped(service, client, req.method, req.args)

    def get_client(self, service, host, port):
        # type: (any, str, int) -> TClient
        # TODO: re-use client
        return make_client(service, host=host, port=port, trans_factory=TFramedTransportFactory())

    def get_file_path(self, thrift_file):
        # type: (str) -> str
        # TODO: ...
        return thrift_file

    def get_service(self, thrift_file, service_name):
        # TODO: reload .thrift file
        thrift = thriftpy.load(self.get_file_path(thrift_file))
        try:
            return getattr(thrift, service_name)
        except AttributeError:
            raise ResourceNotFound('service "%s" not found in "%s"' % (service_name, thrift_file))


def get_handler():
    # TODO: cache
    return ThriftHandler()
