#!/usr/bin/env python
# coding=utf-8
# Stan 2021-05-31

import os
import re
import json
import platform
import configparser
from pprint import pformat

try:
    from importlib.metadata import version  # Available since v3.8
except:
    version = lambda x: '...'

from . import main as run


def decode(code, value):
    if code == 'JSON':
        return json.loads(value)
    elif code == 'INT':
        return int(value)
    elif code == 'LIST':
        return [ i.strip() for i in value.split(',') ]
    elif code == 'INTLIST':
        return [ int(i.strip()) for i in value.split(',') ]
    else:
        print("Unknown code:", code)
        return value


def get_version(pkg_name):
    try:
        ver = version(pkg_name)
    except:
        ver = '<not installed>'

    return ver


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Index table data")

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
                        help="specify a collection/table name (default is 'dump')",
                        metavar="name")

    parser.add_argument('--cfiles',
                        help="specify a collection/table name for file info (default is '_files')",
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

    if args.debug:
        print("Command line arguments:")
        for key, value in vars(args).items():
            print(f"- {key:16}: {value}")
        print()

    if args.version:
        print(f"Python     {platform.python_version()}")
        print(f"pymongo    {get_version('pymongo')}")
        print(f"openpyxl   {get_version('openpyxl')}")
        print(f"pyxlsb     {get_version('pyxlsb')}")
        print(f"xlrd       {get_version('xlrd')}")
        print(f"{__package__:10} {get_version(__package__)}")

        return

    if args.filename is None:
        print("Path not specified")

        return

    if not os.path.exists(args.filename):
        raise FileNotFoundError(f"Path not found: '{args.filename}'")

    if os.path.isfile(args.filename):
        dirname = os.path.dirname(args.filename)
    else:
        dirname = args.filename

    # Optionals arguments
    dburi       = args.dburi  or os.getenv("dburi",       "mongodb://localhost")
    dbname      = args.dbname or os.getenv("dbname",      "db1")
    cname       = args.cname  or os.getenv("cname",       "dump")
    cname_files = args.cfiles or os.getenv("cname_files", "_files")
    config_file = args.config or os.getenv("config_file", os.path.join(dirname, "parser.cfg"))

    if args.debug:
        print(f"Config file: '{config_file}'")

    # Read config file specified
    if os.path.isfile(config_file):
        c = configparser.ConfigParser()
        c.read(config_file, encoding="utf8")
        config = dict(c.items("DEFAULT"))
        for key, value in config.items():
            res = re.split("^{{ (.+) }}", value, 1)
            if len(res) == 3:
                _, code, value = res
                config[key] = decode(code, value)

        if args.debug:
            if len(config):
                print("Config:")
                for key, value in config.items():
                    print(f"- {key:20}: {value}")

            else:
                print("Config is empty")

            print()

    else:
        print("Config file does not exist, default configuration will be applied\n")
        config = {}

    res = run(
        args.filename,
        config      = config,
        dburi       = dburi,
#       tls_ca_file = tls_ca_file,
        dbname      = dbname,
        cname       = cname,
        cname_files = cname_files,
        config_file = config_file,
        verbose     = args.verbose,
        debug       = args.debug
    )

    if args.verbose:
        print("Res:", pformat(res, 2, 160), end="\n\n")


if __name__ == '__main__':
    main()
