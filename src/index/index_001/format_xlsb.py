#!/usr/bin/env python
# coding=utf-8
# Stan 2024-11-01

from pyxlsb import open_workbook, convert_date

from ..chunk import chunk
from ..timer import Timer
from .funcs import get_shid_name


def main_yield(filename, db, options={}, **kargs):
    with Timer(f"[ {__name__} ] open_workbook", db.verbose) as t:
        book = open_workbook(filename)

    sheet_list  = options.get('sheets', book.sheets)
    chunk_rows  = options.get('chunk_rows', 5000)

    for name in sheet_list:         # 1-based integer or string
        # 1-based integer and string
        shid, shname = get_shid_name(book.sheets, name)
        if shid is None:
            db.push_task_record('warning', f"Wrong sheed name/id: {name}")

        # get_sheet accepts 1-based integer as well as string
        sh = book.get_sheet(name)

        if db.verbose:
            print(f"Processing: # {shid} ({sh.name}) / dimension: {sh.dimension}")

        for ki, chunk_i in enumerate(chunk(sh.rows(), chunk_rows)):
            records = []

            for kj, row in enumerate(chunk_i):
                row_values = get_row_values(row)
                if row_values:
                    idx = ki * chunk_rows + kj
                    _r = idx + 1

                    record = dict(row=row_values, _r=_r)
                    records.append(record)

            yield records, {
                '_shid': shid,
#               '_shname': sh.name
            }
            records = []        # release memory

        sh.close()
    book.close()


def get_row_values(row):
    values = [parse_val(i.v) for i in row]
    if any(x is not None for x in values):
        return values

    return []


def parse_val(value):
    if isinstance(value, str):
        return value.strip()

    return value
