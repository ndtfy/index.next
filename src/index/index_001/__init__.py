#!/usr/bin/env python
# coding=utf-8
# Stan 2022-02-05

import os
from importlib import import_module


def main(filename, db, config, **kargs):
    verbose = kargs.get('verbose')
    debug   = kargs.get('debug')

    dirname = os.path.dirname(filename)

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
