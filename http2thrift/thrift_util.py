"""
Copied from thriftpy.protocol.json
"""

from __future__ import (unicode_literals, print_function, division, absolute_import)

import nativetypes
from collections import OrderedDict
from typing import Optional, List

from http2thrift.thriftpy.thrift import TType

# TODO: remove nativetypes dependency
INTEGER_CAST = {
    TType.BYTE: lambda x: int(nativetypes.int8(x)),
    TType.I16: lambda x: int(nativetypes.int16(x)),
    TType.I32: lambda x: int(nativetypes.int32(x)),
    TType.I64: lambda x: int(nativetypes.int64(x)),
}
FLOAT = (TType.DOUBLE,)


def get_args_obj(service, method, args_dict):
    args_obj = getattr(service, method + '_args')()
    struct_to_obj(args_dict, args_obj)
    return args_obj


def get_result_obj(service, method):
    return getattr(service, method + '_result')()


def json_value(ttype, val, spec=None):
    if ttype in INTEGER_CAST or ttype in FLOAT:
        return val if val is not None else 0

    if ttype == TType.STRING:
        return val if val is not None else ''

    if ttype == TType.BOOL:
        return True if val else False

    if ttype == TType.STRUCT:
        return struct_to_json(val)

    if ttype in (TType.SET, TType.LIST):
        return list_to_json(val, spec)

    if ttype == TType.MAP:
        return map_to_json(val, spec)


def obj_value(ttype, val, spec=None):
    if ttype in INTEGER_CAST:
        return INTEGER_CAST[ttype](int(val))

    if ttype in FLOAT:
        return float(val)

    if ttype in (TType.STRING, TType.BOOL):
        return val

    if ttype == TType.STRUCT:
        return struct_to_obj(val, spec())

    if ttype in (TType.SET, TType.LIST):
        return list_to_obj(val, spec)

    if ttype == TType.MAP:
        return map_to_obj(val, spec)


def map_to_json(val, spec):
    res = []
    if isinstance(spec[0], int):
        key_type = spec[0]
        key_spec = None
    else:
        key_type, key_spec = spec[0]

    if isinstance(spec[1], int):
        value_type = spec[1]
        value_spec = None
    else:
        value_type, value_spec = spec[1]

    if val is not None:     # may be optional field?
        for k, v in val.items():
            key = json_value(key_type, k, key_spec)
            value = json_value(value_type, v, value_spec)
            res.append(OrderedDict([('key', key), ('value', value)]))

    return res


def map_to_obj(val, spec):
    res = {}
    if isinstance(spec[0], int):
        key_type, key_spec = spec[0], None
    else:
        key_type, key_spec = spec[0]

    if isinstance(spec[1], int):
        value_type, value_spec = spec[1], None
    else:
        value_type, value_spec = spec[1]

    if isinstance(val, dict):   # new map format
        for k, v in val.items():
            res[obj_value(key_type, k, key_spec)] = obj_value(value_type, v, value_spec)
    else:
        for v in val:
            res[obj_value(key_type, v["key"], key_spec)] = obj_value(
                value_type, v["value"], value_spec)

    return res


def list_to_json(val, spec):
    if isinstance(spec, tuple):
        elem_type, type_spec = spec
    else:
        elem_type, type_spec = spec, None

    if val is None:
        return []
    else:
        return [json_value(elem_type, i, type_spec) for i in val]


def list_to_obj(val, spec):
    if isinstance(spec, tuple):
        elem_type, type_spec = spec
    else:
        elem_type, type_spec = spec, None

    return [obj_value(elem_type, i, type_spec) for i in val]


def struct_to_json(val):
    outobj = OrderedDict()
    if val is None:
        return outobj

    for fid in sorted(val.thrift_spec.keys()):
        field_spec = val.thrift_spec[fid]
        field_type, field_name = field_spec[:2]

        if len(field_spec) <= 3:
            field_type_spec = None
        else:
            field_type_spec = field_spec[2]

        v = getattr(val, field_name)

        outobj[field_name] = json_value(field_type, v, field_type_spec)

    return outobj


def struct_to_obj(val, obj):
    for fid, field_spec in obj.thrift_spec.items():
        field_type, field_name = field_spec[:2]

        if len(field_spec) <= 3:
            field_type_spec = None
        else:
            field_type_spec = field_spec[2]

        if field_name in val:
            setattr(obj, field_name,
                    obj_value(field_type, val[field_name], field_type_spec))

    return obj


def get_seq(seq):
    # type: (Optional[List[int]]) -> int
    if seq is None:
        return 0
    else:
        rv = seq[0]
        seq[0] += 1
        return rv


def generate_sample_obj(ttype, obj, spec, seq=None):
    if obj is None:
        if ttype in INTEGER_CAST:
            return get_seq(seq) + 123
        if ttype in FLOAT:
            return (get_seq(seq) + 456) / 10
        if ttype == TType.BOOL:
            return False
        if ttype == TType.STRING:
            return 'str' + str(get_seq(seq))

    if ttype == TType.STRUCT:
        return generate_sample_struct(obj, spec, seq=seq)
    if ttype in (TType.SET, TType.LIST):
        return generate_sample_list(obj, spec, seq=seq)
    if ttype == TType.MAP:
        return generate_sample_map(obj, spec, seq=seq)
    return obj


_NONE = object()


def generate_sample_struct(obj, cls, seq=_NONE):
    obj = obj or cls()
    if seq is _NONE:
        seq = [0]

    for fid, field_spec in cls.thrift_spec.items():
        field_type, field_name = field_spec[:2]

        if len(field_spec) <= 3:
            field_type_spec = None
        else:
            field_type_spec = field_spec[2]

        field = getattr(obj, field_name, None)
        setattr(
            obj, field_name,
            generate_sample_obj(field_type, field, field_type_spec, seq=seq)
        )

    return obj


def generate_sample_list(obj, spec, seq=None):
    obj = obj or []
    assert isinstance(obj, list)

    if isinstance(spec, tuple):
        elem_type, type_spec = spec
    else:
        elem_type, type_spec = spec, None

    obj.append(generate_sample_obj(elem_type, None, type_spec, seq=seq))
    return obj


def generate_sample_map(obj, spec, seq=None):
    obj = obj or dict()
    assert isinstance(obj, dict)

    if isinstance(spec[0], int):
        key_type = spec[0]
        key_spec = None
    else:
        key_type, key_spec = spec[0]

    if isinstance(spec[1], int):
        value_type = spec[1]
        value_spec = None
    else:
        value_type, value_spec = spec[1]

    key = generate_sample_obj(key_type, None, key_spec, seq=seq)
    value = generate_sample_obj(value_type, None, value_spec, seq=seq)
    obj[key] = value
    return obj
