#!/usr/bin/env python
# coding=utf-8
# Stan 2024-12-25

import configparser
import json
import os
import re
import tempfile
# import warnings
from importlib import import_module
from zipfile import ZipFile

from .timer import Timer
from .db import Db
from .utils import get_memory_info
from .print_once import print_once


def main(**kargs):
    """
    Main file processing function.
    """

#   warnings.simplefilter('always', DeprecationWarning)

    # Db object
    db = Db(**kargs)
    if db.debug:
        print(db, end="\n\n")

    # Resolve config path
    filename = kargs.get('filename')
    config   = kargs.get('config')

    fullname = os.path.abspath(filename)
    if os.path.isfile(fullname):
        dirname = os.path.dirname(fullname)
    else:
        dirname = fullname.rstrip('/')

    config_file = config or os.getenv("INDEX_CONFIG") or "parser.cfg"
    if not os.path.isabs(config_file):
        config_file = os.path.join(dirname, config_file)

    if not os.path.isfile(config_file):
        if config:      # Required if argument specified
            raise FileNotFoundError(f"Config not found: '{config_file}'")

        else:
            config_file = None

    parser_options = {}
    if config_file:
        parser_options = load_options(config_file)

    # Handle filename
    if os.path.isfile(filename):
        if db.verbose:
            print(f"=== Filename: {filename} ===")

        # Resolve parser
        external_parser = parser_options.get('external_parser')
        variant = parser_options.get('variant', 1)

        module_name = external_parser or f".index_{variant:03}"
        parser = import_module(module_name, __package__)
        if db.debug:
            print(f"=== Parser: {parser.__file__} ===")

        # Reg task
        saved, t_id = db.reg_task(parser, parser_options)

        filename = os.path.abspath(filename)
        main_file(filename, db, parser, parser_options)

    else:
        if db.verbose:
            print(f"=== Dirname: {filename} ===")

        filename = os.path.abspath(filename)
        main_dir(filename, db, parser_options)


def main_file(filename, db, parser, parser_options):
    cname       = parser_options.get('cname') or db.cname
    file_keys   = parser_options.get('file_keys', {})
    record_keys = parser_options.get('record_keys', {})
    proceed_anyway = parser_options.get('proceed_anyway')
    raise_after_exception = parser_options.get('raise_after_exception')

    upsert_mode = parser_options.get('upsert_mode')
    upsert_keys = parser_options.get('upsert_keys') or \
                  getattr(parser, '__preferred_upsert_keys__', None)

    collection = db[cname]
    dirname = os.path.dirname(filename)

    # iter if archive
    for filename, localname, source in yield_file(filename):
        # Regular file
        if not source:
            name = os.path.basename(filename)

        # Archive item
        else:
            name = filename

        saved, f_id = db.reg_file(name,
            dirname = dirname,
            source = source,
            ** file_keys
        )

#       if db.file_is_processed():
#           if db.verbose:
#               print(f"File already processed, skipping: '{filename}'")
# 
#           if not proceed_anyway:
#               continue

        if upsert_mode:
            db.upsert_pre_handle(collection)

        exception_occurred = False
        with Timer(f"[ main_file({f_id}) ] finished", db.verbose) as t:
            try:
                total = None
                consumption = []

                for records in parser.main(localname, db, parser_options):
                    if isinstance(records, tuple):
#                       warnings.warn("`parser_returns_tuple` is deprecated. Use `parser_returns_records` instead.", DeprecationWarning, stacklevel=2)
                        records, extra = records
                        records = [ dict(record, **extra) for record in records ]

                    if total is None:
                        total = 0

                    if records:
                        consumption.append( dict(len=len(records),
                            memory=get_memory_info()) )

                        if upsert_mode:
                            db.upsert_many(
                                collection,
                                records,
                                upsert_keys = upsert_keys,
                                ** record_keys
                            )

                        else:
                            db.insert_many(
                                collection,
                                records,
                                ** record_keys
                            )

                        if db.debug and not total:     # First iteration
                            print("Cumulative:", end=' ')

                        total += len(records)

                        if db.debug:
                            print(total, end=' ')

                    else:
                        if db.verbose:
                            print("<No records>")

                if db.debug and total:     # New line after Cumulative message
                    print()

                if db.verbose and total:
                    print(f"Total: {total}; Grand total: { collection.estimated_document_count() }")

            except Exception as ex:
                print_once(f"Exception occurred during processing '{filename}': {ex}", key=str(ex))
                exception_occurred = True

                extra = {}
                if isinstance(ex, SyntaxError):
                    extra = dict(
                        filename = ex.filename,
                        lineno   = ex.lineno,
                        offset   = ex.offset,
                        text     = ex.text
                    )                                

                db.push_file_record(
                    "exception",
                    type = type(ex).__name__,
                    name = str(ex),
                    __dev = dict(
                        args = ex.args,
                        ** extra
                    )
                )

                if raise_after_exception:
                    raise

            finally:
                if upsert_mode:
                    db.upsert_post_handle(collection)

        if db.verbose:     # New line after Timer message
            print()

        if not exception_occurred:
            db.push_file_record(
                'skipped' if total is None else 'completed',
                total = total,
                __consumption = consumption,
                elapsed = t.elapsed
            )


def yield_file(filename, extra_info={}):
    _, ext = os.path.splitext(filename)
    ext = ext.lower()

    localname = extra_info.get('localname', filename)
    source    = extra_info.get('source', [])

    if ext == '.zip':
        with tempfile.TemporaryDirectory(dir=tempfile.gettempdir()) as temp_dir:
            with ZipFile(localname) as zipf:
                for info in zipf.infolist():
                    if info.file_size:
                        # Store relative path for an archive item
                        # and basename for a regular file
                        filepath = filename if source else os.path.basename(filename)

                        for filename1, localname1, source1 in yield_file(info.filename, {
                            'localname': zipf.extract(info.filename, path=temp_dir, pwd=None),
                            'source': source + [filepath]
                        }):
                            yield filename1, localname1, source1

    else:
        yield filename, localname, source


def main_dir(dirname, db, parser_options):
    # Resolve parser
    external_parser = parser_options.get('external_parser')
    variant = parser_options.get('variant', 1)

    module_name = external_parser or f".index_{variant:03}"
    parser = import_module(module_name, __package__)
    if db.debug:
        print(f"=== Parser: {parser.__file__} ===")

    # Reg task
    saved, t_id = db.reg_task(parser, parser_options)

    with Timer("[ main_dir ] finished", db.verbose) as t:
        for root, dirs, files in os.walk(dirname):
            for name in files:
                filename = os.path.join(root, name)
                if db.verbose:
                    print(f"Filename: {filename}")

                main_file(filename, db, parser, parser_options)


def load_options(config_file):
    # Read and resolve DEFAULT section
    c = configparser.ConfigParser()
    c.read(config_file, encoding="utf8")

    options = dict(c.items("DEFAULT"))
    for key, value in options.items():
        res = re.split("^{{ (.+) }}", value, 1)
        if len(res) == 3:
            _, code, value = res
            options[key] = decode(code, value)

    return options


def decode(code, value):
    if code == 'JSON':
        return json.loads(value)
    elif code == 'INT':
        return int(value)
    elif code == 'LIST':
        return [ i.strip() for i in value.split(',') ]
    elif code == 'INTLIST':
        return [ int(i.strip()) for i in value.split(',') ]
    else:
        print("Unknown code:", code)
        return value
