from __future__ import (unicode_literals, print_function, division, absolute_import)

from flask import Flask


_APP = None


def create_app():
    app = Flask(__name__)
    return app


def get_app():
    global _APP
    if _APP is None:
        _APP = create_app()

    return _APP
