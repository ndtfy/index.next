#!/usr/bin/env python
# coding=utf-8
# Stan 2024-12-25

import os
import tempfile
from importlib import import_module
from zipfile import ZipFile

from .timer import Timer
from .db import Db


def main(filename, config, **kargs):
#   verbose = kargs.get('verbose')
    debug   = kargs.get('debug')

    # Db object
    db = Db(**kargs)
    if debug:
        print(db, end="\n\n")

    # Resolve variables depending on filetype
    if os.path.isfile(filename):
        dirname = os.path.dirname(filename)
    else:
        dirname = filename

    # Resolve variables
    parentdirname = os.path.dirname(dirname)
    dictionary = {
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

    config.update(kargs)

    if os.path.isfile(filename):
        main_file(filename, db, config)
    else:
        main_dir(filename, db, config)


def main_file(filename, db, config):
    verbose = config.get('verbose')
    debug   = config.get('debug')

    # Resolve variant and run
    with Timer(f"[ {__name__} / main_file ] over", verbose) as t:
        variant = config.get('variant', 1)
        index_module = import_module(f".index_{variant:03}", __package__)

        _, ext = os.path.splitext(filename)
        ext = ext.lower()

        # Check if archive
        if ext == '.zip':
            with tempfile.TemporaryDirectory(dir=tempfile.gettempdir()) as temp_dir:
                if debug:
                    print(f"temp_dir: {temp_dir}")

                with ZipFile(filename) as zipf:
                    for info in zipf.infolist():
                        if info.file_size:
                            tempname = zipf.extract(info.filename, path=temp_dir, pwd=None)
                            index_module.main(info.filename, db, config, {
                                'source': filename,
                                'localname': tempname
                            })

        else:
            index_module.main(filename, db, config)


def main_dir(dirname, db, config):
    verbose = config.get('verbose')
    debug   = config.get('debug')

    # Resolve variant and run
    with Timer(f"[ {__name__} / main_dir ] over", verbose) as t:
        variant = config.get('variant', 1)
        index_module = import_module(f".index_{variant:03}", __package__)

        for root, dirs, files in os.walk(dirname):
            for name in files:
                filename = os.path.join(root, name)
                if verbose:
                    print(f"Filename: {filename}")

                main_file(filename, db, config)
