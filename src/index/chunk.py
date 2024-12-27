#!/usr/bin/env python
# coding=utf-8
# Stan 2021-05-14

from itertools import islice


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())
