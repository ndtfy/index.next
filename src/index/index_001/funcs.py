#!/usr/bin/env python
# coding=utf-8
# Stan 2025-09-27

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
