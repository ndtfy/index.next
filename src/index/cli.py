#!/usr/bin/env python
# coding=utf-8
# Stan 2021-05-31

import argparse
import configparser
import json
import os
import platform
import re

try:
    from importlib.metadata import version  # Available since Python 3.8
except:
    version = None

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
        ver = version(pkg_name) if version else '...'
    except:
        ver = "<not installed>"

    return ver


def resolve_vars(vars, dictionary):
    for key, value in vars.items():
        if isinstance(value, str):
            for dkey, dvalue in dictionary.items():
                value = value.replace(dkey, dvalue)

            vars[key] = value


def debug_vars(vars, level=1):
    kv = vars.items()
    for key, value in kv:
        print(f"{'  ' * level}- {key:16}: {value}")

    if not kv:
        print("  <empty>")

    print()


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

    parser.add_argument('--cfiles',
                        help="specify a collection name for file log (default is '_files')",
                        metavar="name")

    parser.add_argument('--ctasks',
                        help="specify a collection name for task info (default is '_tasks')",
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
        debug_vars(vars(args))

    if args.version:
        print(f"Python     {platform.python_version()}")
        if version:
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

    # Optionals arguments
    dburi       = args.dburi  or os.getenv("INDEX_DBURI")       or "mongodb://localhost"
    dbname      = args.dbname or os.getenv("INDEX_DBNAME")      or "db1"
    cname       = args.cname  or os.getenv("INDEX_CNAME")       or "dump"
    cname_files = args.cfiles or os.getenv("INDEX_CNAME_FILES") or "_files"
    cname_tasks = args.ctasks or os.getenv("INDEX_CNAME_TASKS") or "_tasks"

    # Resolve config path
    fullname = os.path.abspath(args.filename)
    if os.path.isfile(fullname):
        dirname = os.path.dirname(fullname)
    else:
        dirname = fullname.rstrip('/')

    config_file = args.config or os.getenv("config_file")
    if config_file:
        if not os.path.isabs(config_file):
            config_file = os.path.join(dirname, config_file)

        if not os.path.isfile(config_file):
            raise FileNotFoundError(f"Config not found: '{config_file}'")

    else:
        config_file = os.path.join(dirname, "parser.cfg")

    # Read config file
    if os.path.isfile(config_file):
        c = configparser.ConfigParser()
        c.read(config_file, encoding="utf8")

        # Read and resolve parser section
        parser_options = dict(c.items("DEFAULT"))
        for key, value in parser_options.items():
            res = re.split("^{{ (.+) }}", value, 1)
            if len(res) == 3:
                _, code, value = res
                parser_options[key] = decode(code, value)

        if args.debug:
            print(f"Config file '{config_file}', parser section:")
            debug_vars(parser_options)

    else:
        print("Config file is not specified, default parser parameters will be applied\n")
        parser_options = {}

    # Define variables
    parentdirname = os.path.dirname(dirname)
    dictionary = {
        '{DIRNAME}':           dirname,
        '{BASEDIRNAME}':       os.path.basename(dirname),
        '{PARENTDIRNAME}':     parentdirname,
        '{PARENTBASEDIRNAME}': os.path.basename(parentdirname),
    }
    if args.debug:
        print("Dictionary:")
        debug_vars(dictionary)

    # Resolve app config
    cli_arguments = {
        'dburi':       dburi,
#       'tls_ca_file': tls_ca_file,
        'dbname':      dbname,
        'cname':       cname,
        'cname_files': cname_files,
        'cname_tasks': cname_tasks,
        'verbose':     args.verbose,
        'debug':       args.debug
    }

    resolve_vars(cli_arguments, dictionary)
    if args.debug:
        print("Cli parameters resolved:")
        debug_vars(cli_arguments)

    # Resolve parser parameters
    resolve_vars(parser_options, dictionary)
    if args.debug:
        print("Parser parameters resolved:")
        debug_vars(parser_options)

    res = run(
        args.filename,
        config = cli_arguments,
        parser_options = parser_options
    )


if __name__ == '__main__':
    main()
