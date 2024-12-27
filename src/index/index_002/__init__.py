#!/usr/bin/env python
# coding=utf-8
# Stan 2022-02-05

import os
from importlib import import_module

from ..definition import get_definition


def main(filename, db, config, **kargs):
    verbose = kargs.get('verbose')
    debug   = kargs.get('debug')

    # Header description file + the name of the set
    header_definitions = config.get('header_definitions')
    set_name = config.get('set_name', 'key1')

    if not header_definitions:
        raise Exception(f"Option required: header_definitions")

    if not os.path.isabs(header_definitions):
        dirname = os.path.dirname(filename)
        header_definitions = os.path.join(dirname, header_definitions)

    nrows, rows_values, keys = get_definition(
        header_definitions,
        set_name,
        verbose = verbose,
        debug   = debug
    )

    config['nrows']       = nrows
    config['rows_values'] = rows_values
    config['keys']        = keys

    cname = kargs.get('cname', 'plain')
    collection = db[cname]

    saved, f_id = db.reg_file(filename)
    db.push_file_record(
        f_id,
        __name__,
        config = config,
        cname = cname
    )

    try:
        _, ext = os.path.splitext(filename)

        if ext == '.xlsb':
            module = import_module(f".format_xlsb", __package__ )

        elif ext == '.xlsx':
            module = import_module(f".format_xlsx", __package__ )

        else:
            raise Exception(f"Unknown file type: {ext}")

        total = 0
        for records, extra in module.main_yield(
            filename,
            db,
            config,
            f_id,
            **kargs
        ):
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

        if verbose:
            print(f"Total: {total}; Grand total: { collection.estimated_document_count() }")

        db.push_file_record(
            f_id,
            "completed",
            total = total
        )

    except Exception as e:
        db.push_file_record(
            f_id,
            "exception",
            error = True,
            msg = str(e)
        )

        raise
