from __future__ import (unicode_literals, print_function, division, absolute_import)

import traceback

from thriftpy.thrift import TApplicationException, TException
from typing import Any

from http2thrift.thrift_util import struct_to_json, get_args_obj, get_result_obj


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
    return dict(exception=struct_to_json(exc))


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
