from __future__ import (unicode_literals, print_function, division, absolute_import)

import argparse
from collections import OrderedDict
import json
import fnmatch
import pprint
import sys
from typing import Any, Text, List

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

import requests


DEFAULT_HTTP_HOST = '127.0.0.1'
DEFAULT_HTTP_PORT = 5001
DEFAUL_HTTP_SERVER = '%s:%d' % (DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT)


def get_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('-s', '--server', help='thrift server')
    ap.add_argument('-H', '--http', default=DEFAUL_HTTP_SERVER, help='http2thrift server')
    ap.add_argument('-m', '--method', required=True, help='method pattern')
    ap.add_argument('payload', nargs='?', default=None, help='payload')

    return ap.parse_args()


def make_url(host, port, *pathes):
    url = 'http://%s:%d/api/thrift/%s' % (host, port, '/'.join(pathes))
    return urlparse(url).geturl()   # normalize


def resp2dict(res):
    return res.json(object_pairs_hook=OrderedDict)  # dict is ordered


def call_method(http_host, http_port, thrift_host, thrift_port, path, args_dict):
    url = make_url(http_host, http_port, path)
    data = dict(host=thrift_host, port=thrift_port, args=args_dict)
    res = requests.post(url, data=json.dumps(data))
    return resp2dict(res)


def list_services(http_host, http_port):
    url = make_url(http_host, http_port)
    res = requests.get(url)
    return resp2dict(res)


def get_sample(http_host, http_port, path):
    url = make_url(http_host, http_port, path, 'sample')
    res = requests.get(url)
    return resp2dict(res)


def make_query_path(thrift_file, service, method):
    return '%s:%s:%s' % (thrift_file, service, method)


def query_services(services_info, query):
    # type: (Any, Text) -> List[Text]
    """
    :return: list of url path
    """
    rv = []
    file_pat, _, method_pat = query.rpartition(':')

    for entry in services_info['services']:
        path = entry['path']    # type: Text
        if file_pat:
            if not fnmatch.fnmatch(path, file_pat):
                continue

        for svc_name, svcs in entry['services'].items():
            for method in svcs:
                method_name = method['method']
                if fnmatch.fnmatch(method_name, method_pat):
                    rv.append(make_query_path(path, svc_name, method_name))

    return rv


def split_host_port(string, default_host=None, default_port=None):
    host, _, port = string.partition(':')
    host = host or default_host
    port = port or default_port
    return host, int(port)


def error(msg, *args):
    print(msg % args, file=sys.stderr)


def main():
    args = get_args()
    http_host, http_port = split_host_port(
        args.http, default_host=DEFAULT_HTTP_HOST, default_port=DEFAULT_HTTP_PORT)

    services_info = list_services(http_host, http_port)
    matched = query_services(services_info, args.method)

    if len(matched) == 0:
        error('No matching method for "%s"', args.method)
        sys.exit(1)
    elif len(matched) == 1:
        urlpath = matched[0]

        if args.payload is None:
            # show sample payload
            rv = get_sample(http_host, http_port, urlpath)
        else:
            # make thrift call
            if not args.server:
                error('thrift server not specified')
                sys.exit(2)
            thrift_host, thrift_port = split_host_port(args.server)

            args_dict = json.loads(args.payload)
            rv = call_method(http_host, http_port, thrift_host, thrift_port, urlpath, args_dict)

        print(json.dumps(rv, indent=2))     # TODO: colorize
    else:
        error('too many matched methods')
        pprint.pprint(matched)
        sys.exit(3)


if __name__ == '__main__':
    main()
