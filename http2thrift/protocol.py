"""
Copied from thriftpy.protocol.json
"""

from __future__ import (unicode_literals, print_function, division, absolute_import)

from thriftpy.thrift import TType


INTEGER = (TType.BYTE, TType.I16, TType.I32, TType.I64)
FLOAT = (TType.DOUBLE,)


def json_value(ttype, val, spec=None):
    if ttype in INTEGER or ttype in FLOAT:
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

    for k, v in val.items():
        res.append({"key": json_value(key_type, k, key_spec),
                    "value": json_value(value_type, v, value_spec)})

    return res


def list_to_json(val, spec):
    if isinstance(spec, tuple):
        elem_type, type_spec = spec
    else:
        elem_type, type_spec = spec, None

    return [json_value(elem_type, i, type_spec) for i in val]


def struct_to_json(val):
    outobj = {}
    for fid, field_spec in val.thrift_spec.items():
        field_type, field_name = field_spec[:2]

        if len(field_spec) <= 3:
            field_type_spec = None
        else:
            field_type_spec = field_spec[2]

        v = getattr(val, field_name)

        outobj[field_name] = json_value(field_type, v, field_type_spec)

    return outobj
