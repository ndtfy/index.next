#!/usr/bin/env python
# coding=utf-8
# Stan 2021-05-31

import argparse
import platform

from . import main as run


def main():
    parser = argparse.ArgumentParser(description="Index a spreadsheet")

    parser.add_argument('filename',
                        nargs='?',
                        help="specify a path (dir/file)",
                        metavar="file.xlsx")

    parser.add_argument('--dburi',
                        help="specify a database connection (default is 'mongodb://localhost')",
                        metavar="dbtype://username@hostname/[dbname]")

    parser.add_argument('--dbname',
                        help="specify a database name (default is 'db1')",
                        metavar="name")

    parser.add_argument('--cname',
                        help="specify a collection name (default is 'dump')",
                        metavar="name")

    parser.add_argument('--config',
                        help="specify a config file (default is 'parser.cfg' located in the target directory)",
                        metavar="parser.cfg")

    parser.add_argument('--version',
                        action='store_true',
                        help="version")

    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help="verbose mode")

    parser.add_argument('--debug',
                        action='store_true',
                        help="debug mode")

    args = parser.parse_args()

    if args.version:
        print(f"Python   {platform.python_version()}")
        try:
            # Available since Python 3.8
            from importlib.metadata import version
            print(f"{__package__:8} {version('index')}")
            print(f"pymongo  {version('pymongo')}")
            print(f"openpyxl {version('openpyxl')}")
            print(f"pyxlsb   {version('pyxlsb')}")
            print(f"xlrd     {version('xlrd')}")
        except:
            pass
        return

    if not args.filename:
        print("Path not specified")
        return

    params = {k: v for k, v in vars(args).items() if v}
    run(**params)
