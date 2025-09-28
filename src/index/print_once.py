#!/usr/bin/env python
# coding=utf-8
# Stan 2025-09-28

messages = []

def print_once(*args, key=None):
    if not key:
        key = args

    if key in messages:
        return

    print(*args)
    messages.append(key)
