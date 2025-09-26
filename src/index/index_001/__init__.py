#!/usr/bin/env python
# coding=utf-8
# Stan 2022-02-05

"""Default parser for processing of spreadshhet files.
Available file formats: xls, xlsx, xlsm, xlsb.
"""

import os
from importlib import import_module


__build__ = 1
__rev__   = "2025-09-26"


def main(filename, db, parser_options={}):
        _, ext = os.path.splitext(filename)
        ext = ext.lower()

        module = get_by_ext(ext)
        if module:
            for res in module.main_yield(filename, db, parser_options):
                yield res


def get_by_ext(ext):
    module = None

    if ext == '.xlsb':
        module = import_module(f".format_xlsb", __package__ )

    elif ext == '.xlsx' or ext == '.xlsm':
        module = import_module(f".format_xlsx", __package__ )

    elif ext == '.xls':
        module = import_module(f".format_xls", __package__ )

    return module
