#!/usr/bin/env python
# coding=utf-8
# Stan 2021-02-20

from timeit import default_timer


class Timer(object):
    def __init__(self, description="Elapsed time", verbose=True):
        self.description = description
        self.verbose = verbose
        self.timer = default_timer

    def __enter__(self):
        self.start = self.timer()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        end = self.timer()
        self.elapsed = end - self.start
        if self.verbose:
            print(f"{self.description}: {self.elapsed:.2f} sec")


if __name__ == '__main__':
    with Timer() as t:
        pass

    print(t.elapsed)
