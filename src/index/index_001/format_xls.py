#!/usr/bin/env python
# coding=utf-8
# Stan 2025-09-22

from xlrd import open_workbook

from ..chunk import chunk
from ..timer import Timer


def main_yield(filename, db, options):
    with Timer(f"[ {__name__} ] open_workbook", db.verbose) as t:
        book = open_workbook(filename, on_demand=True)

    sheet_names = book.sheet_names()
    sheet_list  = options.get('sheets', sheet_names)
    chunk_rows  = options.get('chunk_rows', 5000)

    for name in sheet_list:         # 1-based integer or string
        # 1-based integer and string
        shid, shname = get_shid_name(sheet_names, name)
        if shid is None:
            db.push_task_record('warning', f"Wrong sheed name/id: {name}")

        sh = book.sheet_by_name(shname)     # string

        if db.verbose:
            print(f"Processing: # {shid} ({sh.name}) / nrows: {sh.nrows}, ncols: {sh.ncols}")

        for ki, chunk_i in enumerate(chunk(get_rows(sh), chunk_rows)):
            records = []

            for kj, row in enumerate(chunk_i):
                if row:
                    idx = ki * chunk_rows + kj
                    _r = idx + 1
    
                    record = dict(row=row, _r=_r)
                    records.append(record)

            yield records, {
                '_shid': shid,
#               '_shname': sh.name
            }
            records = []        # release memory

        book.unload_sheet(shname)

    book.release_resources()


def get_shid_name(sheet_names, name):
    if isinstance(name, int):
        if len(sheet_names) < name:
            return None, None

        shid0 = name - 1
        name = sheet_names[shid0]

    else:
        if name not in sheet_names:
            return None, None

        shid0 = sheet_names.index(name)

    return shid0 + 1, name


def get_rows(sh):
    for y in range(sh.nrows):
        yield get_row_values(sh, y)


def get_row_values(sh, y):
    values = [get_strip(x) for x in sh.row_values(y)]
    if any(x != '' for x in values):
        return values

    return []


def get_strip(s):
    if isinstance(s, str):
        return s.strip()

    return s
