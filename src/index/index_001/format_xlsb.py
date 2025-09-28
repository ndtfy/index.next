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
    row_mode    = options.get('row_mode', 1)
    cells_mode  = options.get('cells_mode', 0)

    processed = []

    for name in sheet_list:         # 1-based integer or string
        # 1-based integer and string
        shid, shname = get_shid_name(book.sheets, name)
        if shid is None:
            db.push_file_record('warning',
                message = f"Wrong sheet name: {name}"
            )
            continue

        if shid in processed:
            db.push_file_record('info',
                message = f"Sheet already processed: {name}"
            )
            continue

        processed.append(shid)

        # get_sheet accepts 1-based integer as well as string
        sh = book.get_sheet(name)

        if db.verbose:
            print(f"Processing: # {shid} ({sh.name}) / dimension: {sh.dimension}")

        for ki, chunk_i in enumerate(chunk(sh.rows(), chunk_rows)):
            records = []

            for kj, row in enumerate(chunk_i):
                record = {}
                if row_mode:
                    record['row'] = get_row_values(row)

                if cells_mode:
                    record['_cells'] = get_cells(row)

                record = {k: v for k, v in record.items() if v}
                if record:
                    idx = ki * chunk_rows + kj
                    _r = idx + 1

                    record = dict(**record, _r=_r)
                    records.append(record)

            yield records, {
                '_shid': shid,
#               '_shname': sh.name
            }
            records = []        # release memory

        sh.close()
    book.close()


# Plain mode
def get_row_values(row):
    values = [parse_val(cell.v) for cell in row]

    if any(x is not None for x in values):
        return values


def parse_val(value):
    if isinstance(value, str):
        value = value.strip()

    return value


# Attribute pattern
def get_cells(row):
    values = [parse_val_ext(cell.v, col_i) for col_i, cell in enumerate(row, 1)]
    values = [x for x in values if x is not None]

    return values


def parse_val_ext(value, col_i):
    if isinstance(value, str):
        value = value.strip()

    if value is not None:
        return {
            'c': col_i,
            'v': value
        }
