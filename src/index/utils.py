#!/usr/bin/env python
# coding=utf-8
# Stan 2025-10-05

import os
from datetime import datetime

try:
    import psutil
    process = psutil.Process()

except ModuleNotFoundError:
    process = None


def get_file_info(filename):
    try:
        timestamp = os.path.getmtime(filename)
        size = os.path.getsize(filename)
        if os.name == 'nt':
            mtime = datetime.fromtimestamp(timestamp)
        else:
            mtime = datetime.utcfromtimestamp(timestamp)

        return dict(
            mtime = mtime,
            timestamp = int(timestamp),
            size = size
        )
    except:
        return {}


def get_memory_info():
    if process:
        return skip_exc(lambda: process.memory_info()._asdict())
    else:
        return "`psutil` must be installed"


def skip_exc(func, default=None):
    try:
        return func()
    except:
        return default
