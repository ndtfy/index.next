#!/usr/bin/env python
# coding=utf-8
# Stan 2022-02-05

"""Default parser for processing of spreadsheet files.
Available file formats: xlsx, xlsm, xlsb, xls.
"""

import os
from importlib import import_module


__build__ = 1
__rev__ = 20251005

__preferred_upsert_keys__ = ['_row', '_shid', '_r']


def main(filename, db, options={}, **kargs):
    _, ext = os.path.splitext(filename)
    ext = ext.lower()

    module = get_by_ext(ext)
    if module:
        for res in module.main_yield(filename, db, options):
            yield res


def get_by_ext(ext):
    module = None

    if ext == '.xls':
        module = import_module(".format_xls", __package__ )

    elif ext == '.xlsb':
        module = import_module(".format_xlsb", __package__ )

    elif ext == '.xlsx' or ext == '.xlsm':
        module = import_module(".format_xlsx", __package__ )

    return module
