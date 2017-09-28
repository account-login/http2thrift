from __future__ import (unicode_literals, print_function, division, absolute_import)

import json

from flask import request, abort, make_response

from http2thrift.flask_app import get_app
from http2thrift.thrift_handler import get_handler, ThriftRequest, ResourceNotFound


app = get_app()


@app.route('/api/thrift/<path:thrift_file>:<service>:<method>', methods=['POST'])
def thrift_call(thrift_file, service, method):
    req_dict = json.loads(request.get_data(as_text=True))
    host = req_dict.get('host', '127.0.0.1')
    port = req_dict.get('port', 0)
    if port == 0:
        pass
    args_dict = req_dict.get('args', dict())

    req = ThriftRequest(
        host=host, port=port,
        thrift_file=thrift_file, service=service, method=method, args=args_dict)

    try:
        result = get_handler().call(req)
    except ResourceNotFound:
        abort(404)
    else:
        resp = make_response(json.dumps(result, indent=4))
        resp.headers['Content-Type'] = 'application/json; charset=utf-8'
        return resp
