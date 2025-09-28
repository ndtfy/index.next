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
    row_mode    = options.get('row_mode', 1)
    cells_mode  = options.get('cells_mode', 0)

    processed = []

    for name in sheet_list:         # 1-based integer or string
        # 1-based integer and string
        shid, shname = get_shid_name(sheet_names, name)
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

        sh = book.sheet_by_name(shname)     # string
#       sh = book.sheet_by_index(shid)      # 0-based

        if db.verbose:
            print(f"Processing: # {shid} ({sh.name}) / nrows: {sh.nrows}, ncols: {sh.ncols}")

        notes = [(k[0], k[1], v) for k, v in sh.cell_note_map.items()]

        for ki, chunk_i in enumerate(chunk(sh.get_rows(), chunk_rows)):
            records = []

            for kj, row in enumerate(chunk_i):
                idx = ki * chunk_rows + kj
                _r = idx + 1

                notes_i = list(filter(lambda x: x[0] == idx, notes))

                record = {}
                if row_mode:
                    record['row'] = get_row_values(row)

                if cells_mode:
                    record['_cells'] = get_cells(row, notes_i)

                record = {k: v for k, v in record.items() if v}
                if record:
                    record = dict(**record, _r=_r)
                    records.append(record)

            yield records, {
                '_shid': shid,
#               '_shname': sh.name
            }
            records = []        # release memory

        book.unload_sheet(shname)
    book.release_resources()


# Plain mode
def get_row_values(row):
    values = [parse_cell(cell) for cell in row]

    if any(x is not None for x in values):
        return values


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


# Attribute pattern
def get_cells(row, notes):
    values = []
    for col_i, cell in enumerate(row, 1):
        value = parse_cell_ext(cell, col_i)

        note_dict = get_note(notes, col_i)
        if note_dict:
            if value:   # dict
                value['_n'] = note_dict

            else:
                value = dict(c=col_i, _n=note_dict)

        values.append(value)
    values = [x for x in values if x is not None]

    return values


def parse_cell_ext(cell, col_i):
    if cell.ctype == xlrd.XL_CELL_EMPTY:
        return None

    if cell.ctype == xlrd.XL_CELL_TEXT:
        val = cell.value.strip()
        return {
            'c': col_i,
            'v': val,
            't': 1
        }

    if cell.ctype == xlrd.XL_CELL_NUMBER:
        val = cell.value
        return {
            'c': col_i,
            'v': val,
            't': 2
        }

    if cell.ctype == xlrd.XL_CELL_DATE:
        val = xlrd.xldate.xldate_as_datetime(cell.value, 0)
        return {
            'c': col_i,
            'v': val,
            't': 3
        }

    if cell.ctype == xlrd.XL_CELL_BOOLEAN:
        val = bool(cell.value)
        return {
            'c': col_i,
            'v': val,
            't': 4
        }

    if cell.ctype == xlrd.XL_CELL_ERROR:
        val = cell.value
        return {
            'c': col_i,
            'v': val,
            't': 5
        }

    val = cell.value
    return {
        'c': col_i,
        'v': val,
        't': -cell.ctype   # trick for rare type
    }


def get_note(notes, col_i):
    note_list = list(filter(lambda x: x[1] == col_i - 1, notes))
    note = note_list[0][2] if note_list else None
    if note:
        return dict(
            author = note.author,
            show = note.show,
            text = note.text
        )
