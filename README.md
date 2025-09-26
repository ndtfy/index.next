# index

*Read this in other languages: [English](README.md), [Русский](README.ru-RU.md)*

A lightweight spreadsheet indexing system.

[![PyPI - Version](https://img.shields.io/pypi/v/index?color=%23d0d0ff)](https://pypi.org/project/index/)
![PyPI - Status](https://img.shields.io/pypi/status/index)
![PyPI - Downloads](https://img.shields.io/pypi/dm/index)

## Installation & Configuration

> `index` requires **Python 3.6.0 or higher** and **pip 19.0 or higher**.

To install, use the following command:

```bash
pip install --pre index
```

This will automatically install all necessary packages for operation: `openpyxl`, `pyxlsb`, `pymongo`.

If you also need to index legacy `*.xls` format files, use the command:

```bash
pip install --pre index[obsolete]
```

## Quick Start

```bash
index file.xlsx
```

The file contents will be read and saved to a MongoDB database.

Default parameters:
```
dburi = mongodb://localhost
dbname = db1
cname = dump
cname_files = _files
```

## Project Status

`Development Status :: 4 - Beta`

## About the Project

The project was originally located here: [https://github.com/ndtfy/index](https://github.com/ndtfy/index). Over time, the project's architecture was changed, and the project has moved.

## Useful Links

*   [https://pypi.org/project/index/](https://pypi.org/project/index/)

## License

*   MIT