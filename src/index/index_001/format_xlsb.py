#!/usr/bin/env python
# coding=utf-8
# Stan 2024-11-01

from pyxlsb import open_workbook

from ..chunk import chunk
from ..timer import Timer


def main_yield(filename, db, config, f_id, **kargs):
    verbose = kargs.get('verbose')

    with Timer(f"[ {__name__} ] open_workbook '{filename}'", verbose) as t:
        book = open_workbook(filename)

    sheet_list  = config.get('sheets', book.sheets)
    chunk_rows  = config.get('chunk_rows', 5000)

    for index in sheet_list:            # integer or string
        sh = book.get_sheet(index)      # index is 1-based

        if verbose:
            print(f"Processing: # {index} ({sh.name}) / dimension: {sh.dimension}")

        for ki, chunk_i in enumerate(chunk(sh.rows(), chunk_rows)):
            records = []

            for kj, row in enumerate(chunk_i):
                idx = ki * chunk_rows + kj
                _r = idx + 1

                row_values = [i.v for i in row]
                record = dict(row=row_values, _r=_r)
                records.append(record)

            yield records, {
                '_shid': index,
#               '_shname': sh.name
            }
            records = []
