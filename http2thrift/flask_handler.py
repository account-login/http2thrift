from __future__ import (unicode_literals, print_function, division, absolute_import)

import json
from functools import wraps

from flask import request, make_response

from http2thrift.flask_app import get_app
from http2thrift.thrift_handler import get_handler, ThriftRequest, ResourceNotFound


app = get_app()
get_handler()   # init thrift handler


def json_response(dct, code=200):
    resp = make_response(json.dumps(dct, indent=4))
    resp.status_code = code
    resp.headers['Content-Type'] = 'application/json; charset=utf-8'
    return resp


def error_response(msg, code):
    return json_response(dict(error=msg), code=code)


def json_api(f):
    @wraps(f)
    def g(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
        except ResourceNotFound as exc:
            return error_response(str(exc), 404)
        else:
            return json_response(result)

    return g


@app.route('/api/thrift/<path:thrift_file>:<service>:<method>', methods=['POST'])
@json_api
def thrift_call(thrift_file, service, method):
    req_dict = json.loads(request.get_data(as_text=True))
    host = req_dict.get('host', '127.0.0.1')
    port = req_dict.get('port', 0)
    if port == 0:
        return error_response('"port" is required', 400)
    args_dict = req_dict.get('args', dict())

    req = ThriftRequest(
        host=host, port=port,
        thrift_file=thrift_file, service=service, method=method, args=args_dict)
    return get_handler().call(req)


@app.route('/api/thrift/', methods=['GET'])
@app.route('/api/thrift/<path:thrift_file>', methods=['GET'])
@json_api
def list_services(thrift_file=None):
    info = get_handler().list_services(thrift_file)
    return dict(services=info)


@app.route('/api/thrift/<path:thrift_file>:<service>:<method>/sample', methods=['GET'])
@json_api
def thrift_sample(thrift_file, service, method):
    return get_handler().get_sample(thrift_file, service, method)
