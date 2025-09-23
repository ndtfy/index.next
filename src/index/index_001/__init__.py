#!/usr/bin/env python
# coding=utf-8
# Stan 2022-02-05

import os
from importlib import import_module
from tempfile import mkstemp


def main(filename, db, config, extra_info={}):
    cname   = config.get('cname', 'plain')
    verbose = config.get('verbose')
    debug   = config.get('debug')

    collection = db[cname]

    f_id = None
    try:
        source = extra_info.get('source')
        localname = extra_info.get('localname', filename)

        _, ext = os.path.splitext(filename)
        ext = ext.lower()

        module = get_by_ext(ext)
        if not module:
            if debug:
                wrapper = f" (source {source})" if source else ''
                print(f"Skipping file: {filename}{wrapper}")

            return

        f_id = reg_file(filename, db, config, cname, source)

        total = 0
        for records, extra in module.main_yield(localname, config):
            if len(records):
                db.insert_many(
                    collection,
                    records,
                    verbose,
                    _fid = f_id,
                    ** extra,
                    ** config.get('record_keys', {})
                )
                total += len(records)
                if debug:
                    print("Cumulative:", total)
    
            else:
                if verbose:
                    print("Empty list")
    
        if total and verbose:
            print(f"Total: {total}; Grand total: { collection.estimated_document_count() }")
    
        db.push_file_record(
            f_id,
            "completed",
            total = total
        )

    except Exception as ex:
        if f_id:
            db.push_file_record(
                f_id,
                "exception",
                type = type(ex).__name__,
                args = ex.args
            )
        else:
            raise

        if False:
            print(e)
        else:
            raise


def get_by_ext(ext):
    module = None

    if ext == '.xlsb':
        module = import_module(f".format_xlsb", __package__ )

    elif ext == '.xlsx':
        module = import_module(f".format_xlsx", __package__ )

    elif ext == '.xls':
        module = import_module(f".format_xls", __package__ )

    return module


def reg_file(filename, db, config, cname, source=None):
    saved, f_id = db.reg_file(filename, source)

    # Shield the connection parameters
    config = {k: v for k, v in config.items() if k[0:2] != 'db'}

    db.push_file_record(f_id, __name__, config = config, cname = cname)

    return f_id
