from __future__ import (unicode_literals, print_function, division, absolute_import)

import logging


# TODO: requirements.txt
# TODO: non-utf8 thrift file


def get_logger(name):
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)7s %(asctime)s.%(msecs)03d [%(module)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    return logging.getLogger(name)
