#!/usr/bin/env python
# coding=utf-8
# Stan 2021-02-27

from datetime import datetime

import pymongo

from ..timer import Timer


class Db():
    def __init__(self, dburi,
        dbname      = 'db1',
        cname_files = '_files',
        cname_tasks = '_tasks',
        tls_ca_file = None,
        verbose     = False,
        **kargs
    ):
        self.dburi       = dburi
        self.tls_ca_file = tls_ca_file
        self.dbname      = dbname
        self.cname_files = cname_files
        self.cname_tasks = cname_tasks
        self.verbose     = verbose

        self.current_file = None
        self.current_task = None

        self.client = pymongo.MongoClient(
            dburi,
            tlsCAFile = tls_ca_file,
        )
        self.db = self.client[dbname]


    def __str__(self):
        server_info = self.client.server_info()
        return f"MongoDB: [version: {server_info['version']}; sysInfo: {server_info['sysInfo']}; ok: {server_info['ok']}]"


    def __getitem__(self, cname):
        return self.db[cname]


    def insert_many(self, collection, record_list, **kargs):
        if kargs:
            record_list = list(map(lambda item: dict(item, **kargs), record_list))

        if self.verbose:
            print(f"[ { datetime.utcnow() } ]: inserting started ({ len(record_list) } records)...", end=" ")

        with Timer("completed", self.verbose) as t:
            res = collection.insert_many(record_list)

        return res


    def upsert_many(self, collection, record_list, key_list=None, **kargs):
        if kargs:
            record_list = list(map(lambda item: dict(item, **kargs), record_list))

        if key_list:
            upserts = [ pymongo.UpdateOne({k: v for k, v in x.items() if k in key_list}, {'$set': x}, upsert=True) for x in record_list ]

        else:
            upserts = [ pymongo.UpdateOne({k: v for k, v in x.items() if not k.startswith('_')}, {'$set': x}, upsert=True) for x in record_list ]

        if self.verbose:
            print(f"[ { datetime.utcnow() } ]: upserting started ({ len(record_list) } records)...", end=" ")

        with Timer("completed", self.verbose) as t:
            res = collection.bulk_write(upserts)

        if self.verbose:
            print("deleted: %s / inserted: %s / matched: %s / modified: %s / upserted: %s" % (
                res.deleted_count,
                res.inserted_count,
                res.matched_count,
                res.modified_count,
                res.upserted_count)
            )

        return res


    # Methods for _tasks collection

    def reg_task(self, parser, parser_options, **kargs):
        dt = datetime.utcnow()

        collection = self.db[self.cname_tasks]

        amended = {k: v for k, v in kargs.items() if not is_empty(v)}
        task_dict   = dict(
            name    = parser.__name__,
            build   = parser.__build__,     # User specified
            rev     = parser.__rev__,       # User specified
            options = parser_options,
            **amended
        )

        res = collection.find_one(task_dict, {'_id': 1})
        if res:
            return True, res['_id']

        record_dict = dict(
            **task_dict,
            doc = parser.__doc__,
            dev = {
                'package': parser.__package__,
                'file':    parser.__file__
            },
            created = dt
        )
        res = collection.insert_one(record_dict)

        self.current_task = res.inserted_id

        return False, res.inserted_id


    def push_task_record(self, action, **kargs):
        dt = datetime.utcnow()

        collection = self.db[self.cname_tasks]

        amended = {k: v for k, v in kargs.items() if not is_empty(v)}

        return collection.update_one(
            { '_id': self.current_task },
            {
                '$set': {
                    'updated': dt,
                },
                '$push': {
                    'records': dict(
                        action = action,
                        **amended,
                        created = dt
                    )
                }
            }
        )


    # Methods for _files collection

    def reg_file(self, filename, **kargs):
        dt = datetime.utcnow()

        collection = self.db[self.cname_files]

        amended = {k: v for k, v in kargs.items() if not is_empty(v)}
        amended = {k: ' => '.join(v) if isinstance(v, (tuple, list)) else v \
                   for k, v in amended.items()}
        file_dict = dict(name = filename, **amended)

        res = collection.find_one(file_dict, {'_id': 1})
        if res:
            return True, res['_id']

        record_dict = dict(
            **file_dict,
            created = dt
        )
        res = collection.insert_one(record_dict)

#       self.current_file = res.inserted_id

        return False, res.inserted_id


    def push_file_record(self, _id, action, **kargs):
        dt = datetime.utcnow()

        collection = self.db[self.cname_files]

        amended = {k: v for k, v in kargs.items() if not is_empty(v)}

        return collection.update_one(
            { '_id': _id },
            {
                '$set': {
                    'updated': dt,
                },
                '$push': {
                    'records': dict(
                        action = action,
                        **amended,
                        created = dt
                    )
                }
            }
        )


# Utilities

def is_empty(v):
    if v:
        return False

    if v is None:
        return True

    if isinstance(v, (dict, list, tuple, str)):
        return True
