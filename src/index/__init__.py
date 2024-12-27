#!/usr/bin/env python
# coding=utf-8
# Stan 2024-12-25

import os
from importlib import import_module

from .timer import Timer
from .db import Db


def main(filename, config, **kargs):
    verbose = kargs.get('verbose')
    debug   = kargs.get('debug')

    # Resolve variables
    _, ext = os.path.splitext(filename)
    dirname = os.path.dirname(filename)
    parentdirname = os.path.dirname(dirname)
    dictionary = {
        '$EXT':               ext,
        '$BASENAME':          os.path.basename(filename),
        '$DIRNAME':           dirname,
        '$BASEDIRNAME':       os.path.basename(dirname),
        '$PARENTDIRNAME':     parentdirname,
        '$PARENTBASEDIRNAME': os.path.basename(parentdirname),
    }

    if debug:
        print("Dictionary:")
        for key, value in dictionary.items():
            print(f"- {key:20}: {value}")
        print()

    for key, value in kargs.items():
        if isinstance(value, str):
            for dkey, dvalue in dictionary.items():
                value = value.replace(dkey, dvalue)

            kargs[key] = value

    if debug:
        print("Keyword arguments:")
        for key, value in kargs.items():
            print(f"- {key:16}: {value}")
        print()

    # Db object
    db = Db(**kargs)
    if debug:
        print(db, end="\n\n")

    # Resolve variant and run
    with Timer(f"[ {__name__} ]", verbose) as t:
        variant = config.get('variant', 1)
        index_module = import_module(f".index_{variant:03}", __package__)
        index_module.main(filename, db, config, **kargs)
