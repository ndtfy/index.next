#!/usr/bin/env python
# coding=utf-8
# Stan 2025-09-22

import xlrd

from ..chunk import chunk
from ..timer import Timer
from .funcs import get_shid_name


def main_yield(filename, db, options={}, **kargs):
    with Timer(f"[ {__name__} ] open_workbook", db.verbose) as t:
        book = xlrd.open_workbook(filename, on_demand=True)

    sheet_names = book.sheet_names()
    sheet_list  = options.get('sheets', sheet_names)
    chunk_rows  = options.get('chunk_rows', 5000)

    for name in sheet_list:         # 1-based integer or string
        # 1-based integer and string
        shid, shname = get_shid_name(sheet_names, name)
        if shid is None:
            db.push_task_record('warning', f"Wrong sheed name/id: {name}")

        sh = book.sheet_by_name(shname)     # string
#       sh = book.sheet_by_index(shid)      # 0-based

        if db.verbose:
            print(f"Processing: # {shid} ({sh.name}) / nrows: {sh.nrows}, ncols: {sh.ncols}")

        for ki, chunk_i in enumerate(chunk(sh.get_rows(), chunk_rows)):
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

        book.unload_sheet(shname)
    book.release_resources()


def get_row_values(row):
    values = [parse_cell(i) for i in row]
    if any(x is not None for x in values):
        return values

    return []


def parse_cell(cell):
    if cell.ctype == xlrd.XL_CELL_EMPTY or \
       cell.ctype == xlrd.XL_CELL_BLANK:
        return None

    if cell.ctype == xlrd.XL_CELL_TEXT:
        return cell.value.strip()

    if cell.ctype == xlrd.XL_CELL_NUMBER:
        return cell.value

    if cell.ctype == xlrd.XL_CELL_DATE:
        return xlrd.xldate.xldate_as_datetime(cell.value, 0)

    if cell.ctype == xlrd.XL_CELL_BOOLEAN:
        return bool(cell.value)

    if cell.ctype == xlrd.XL_CELL_ERROR:
        return f"ERROR({cell.value})"

    return cell.value
