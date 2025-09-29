#!/usr/bin/env python
# coding=utf-8
# Stan 2024-12-25

import sys
import argparse
import os

from dotenv import load_dotenv

from .cli import main


# Parse --env argument
parser = argparse.ArgumentParser(
    prog="python -m index",
    description="Index a spreadsheet using a dotenv file",
    epilog="To display all arguments, use the command: index --help"
)

parser.add_argument('--env',
    help="specify a dotenv file (default is '.env' located in the current directory)",
    metavar=".env")

args, remaining = parser.parse_known_args()

if '--debug' in remaining:
    print(f"Command line arguments: {sys.argv[1:]}")
    print(f"Retrieved arguments: {args}")
    print(f"Remaining arguments: {remaining}")

sys.argv = [sys.argv[0]] + remaining


# Resolve a dotenv file
env_file = args.env
if env_file:
    if not os.path.isfile(env_file):
        raise FileNotFoundError(f"A dotenv file not found: '{env_file}'")

else:
    env_file = ".env"

if '--debug' in remaining:
    print(f"A dotenv file: {env_file}")


# Load environment variables
succeed = load_dotenv(env_file)
if '--debug' in remaining:
    print(f"load_dotenv('{env_file}') -> {succeed}\n")


main()
