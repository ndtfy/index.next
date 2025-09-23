#!/usr/bin/env python
# coding=utf-8
# Stan 2025-09-22

from xlrd import open_workbook

from ..chunk import chunk
from ..timer import Timer


def main_yield(filename, config):
    verbose = config.get('verbose')

    with Timer(f"[ {__name__} ] open_workbook '{filename}'", verbose) as t:
        book = open_workbook(filename, on_demand=True)

    sheet_names = book.sheet_names()
    sheet_list  = config.get('sheets', sheet_names)
    chunk_rows  = config.get('chunk_rows', 5000)

    for name in sheet_list:            # integer or string
#       shid, shname = get_shid_name(sheet_names, name)
        sh = book.sheet_by_name(name)
        seq = sheet_names.index(name)

        if verbose:
            print(f"Processing: # {name} ({sh.name}) / nrows: {sh.nrows}, ncols: {sh.ncols}")

        for ki, chunk_i in enumerate(chunk(get_rows(sh), chunk_rows)):
            records = []

            for kj, row in enumerate(chunk_i):
                if row:
                    idx = ki * chunk_rows + kj
                    _r = idx + 1
    
                    record = dict(row=row, _r=_r)
                    records.append(record)

            yield records, {
                '_shid': name,
#               '_shname': sh.name
            }
            records = []

        book.unload_sheet(name)

    book.release_resources()


# def get_shid_name(sheet_names, name):
#     if isinstance(name, int):
#         shid = name - 1
#         name = sheet_names[shid]
#     else:
#         shid = sheet_names.index(name)
# 
#     return shid, name


def get_strip(s):
    if isinstance(s, str):
        return s.strip()

    if s is None:
        return ''

    return s


def get_row(sh, y):
    values = [get_strip(x) for x in sh.row_values(y)]
    if any(x != '' for x in values):
        return values

    return []


def get_rows(sh):
    for y in range(sh.nrows):
        yield get_row(sh, y)
