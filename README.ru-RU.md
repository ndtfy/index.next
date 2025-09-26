# index

*Read this in other languages: [English](README.md), [Русский](README.ru-RU.md)*

Легковесная система индексации электронных таблиц.

[![PyPI - Version](https://img.shields.io/pypi/v/index?color=%23d0d0ff)](https://pypi.org/project/index/)
![PyPI - Status](https://img.shields.io/pypi/status/index)
![PyPI - Downloads](https://img.shields.io/pypi/dm/index)

## Установка и настройка

> Для работы `index` требуется **Python 3.6.0 или выше** и **pip 19.0 или выше**.

Для установки используйте команду:

```bash
pip install --pre index
```

При этом будут автоматически установлены все необходимые для работы пакеты: `openpyxl`, `pyxlsb`, `pymongo`.

Если также необходимо индексирование файлов старого формата `*.xls`, используйте команду:

```bash
pip install --pre index[obsolete]
```

## Быстрый запуск

```bash
index file.xlsx
```

Содержимое файла будет прочитано и сохранено в базу данных MongoDB.

Параметры по умолчанию:
```
dburi = mongodb://localhost
dbname = db1
cname = dump
cname_files = _files
```

## Статус проекта

`Development Status :: 4 - Beta`

## Немного о проекте

Изначально проект располагался здесь: [https://github.com/ndtfy/index](https://github.com/ndtfy/index). Со временем архитектура проекта была изменена, и проект переехал.

## Полезные ссылки

*   [https://pypi.org/project/index/](https://pypi.org/project/index/)

## Лицензия

*   MIT