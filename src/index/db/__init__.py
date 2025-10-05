#!/usr/bin/env python
# coding=utf-8
# Stan 2021-02-27

from datetime import datetime

import pymongo

from ..timer import Timer
from ..utils import get_file_info


class Db():
    def __init__(self, dburi,
        dbname      = None,
        cname_files = '_files',
        cname_tasks = '_tasks',
        tls_ca_file = None,
        verbose     = False,
        debug       = False,
        ** kargs
    ):
        self.dburi       = dburi
        self.tls_ca_file = tls_ca_file
        self.cname_files = cname_files
        self.cname_tasks = cname_tasks
        self.verbose     = verbose
        self.debug       = debug

        self.current_file = None
        self.current_task = None

        self.client = pymongo.MongoClient(
            dburi,
            tlsCAFile = tls_ca_file,
#           serverSelectionTimeoutMS = 10000
        )

        if not dbname:
            self.db = self.client.get_default_database('db1')
            dbname = self.db.name

        else:
            self.db = self.client[dbname]

        self.dbname = dbname


    def __str__(self):
        server_info = self.client.server_info()

        version = server_info.get('version', '')
        ok      = server_info.get('ok', '')
        build   = server_info.get('buildEnvironment', {})
        distmod  = build.get('distmod', '')
        distarch = build.get('distarch', '')

        return f"MongoDB server: version: {version}; dist: '{distmod}/{distarch}'; ok: {ok} / dbname: '{self.dbname}'"


    def __getitem__(self, cname):
        return self.db[cname]


    def insert_many(self, collection, record_list, **kargs):
        now = datetime.utcnow()

        extra = dict(kargs, _fid=self.current_file)
        record_list = [ dict(record, **extra) for record in record_list ]

        if self.debug:
            print(f"[ {now} ]: inserting started ({ len(record_list) } records)...", end=" ")

        with Timer("completed", self.verbose) as t:
            res = collection.insert_many(record_list)

        return res


    def upsert_many(self, collection, record_list, upsert_keys=None, **kargs):
        now = datetime.utcnow()

        if not upsert_keys:
            upsert_keys = record_list[0].keys()

        upserts = [ pymongo.UpdateOne(
            filter = {k: v for k, v in x.items() if k in upsert_keys},
            update = {
                "$currentDate": {
                  "updated": True,
                },
                "$push": {
                    "_extra": {
                        "_tid": self.current_task,
                        "_fid": self.current_file,
                        ** {k: v for k, v in x.items() if not k in upsert_keys},
                        ** kargs,
                        "scanned": now,
                    },
                },
                "$inc": { "_v": 1 },
                "$setOnInsert": {
                    "created": now,
                },
            },
            upsert = True
        ) for x in record_list ]

        if self.debug:
            print(f"[ {now} ]: upserting started ({ len(record_list) } records)...", end=" ")

        with Timer("completed", self.verbose) as t:
            res = collection.bulk_write(upserts)

        if self.debug:
            print("deleted: %s / inserted: %s / matched: %s / modified: %s / upserted: %s" % (
                res.deleted_count,
                res.inserted_count,
                res.matched_count,
                res.modified_count,
                res.upserted_count)
            )

        # Remove the `deleted` flag for existing records
        # Feature: `deleted` flag will be relative to the last scan
        updates = [ pymongo.UpdateOne(
            filter = {
                ** {k: v for k, v in x.items() if k in upsert_keys},
                "_extra": {
                    "$elemMatch": {
                        "_tid": self.current_task,
                        "_fid": self.current_file,
                        "deleted": True,
                    }
                },
            },
            update = {
                "$unset": { "_extra.$[elem].deleted": "" },
            },
            array_filters = [
                {
                    "elem._tid": self.current_task,
                    "elem._fid": self.current_file,
                    ** {f"elem.{k}": v for k, v in x.items() if not k in upsert_keys},
                }
            ]
        ) for x in record_list ]
  
        collection.bulk_write(updates)

        return res


    def upsert_pre_handle(self, collection):
        collection.update_many(
            filter = {
                "_extra": {
                    "$elemMatch": {
                        "_tid": self.current_task,
                        "_fid": self.current_file,
                    }
                },
            },
            update = {
                "$set": { "_extra.$[elem].deleted": True },
            },
            array_filters = [
                {
                    "elem._tid": self.current_task,
                    "elem._fid": self.current_file,
                }
            ]
        )


    def upsert_post_handle(self, collection):
        pass


    # Methods for _tasks collection

    def reg_task(self, parser, parser_options, **kargs):
        now = datetime.utcnow()

        collection = self.db[self.cname_tasks]

        amended = {k: v for k, v in kargs.items() if not is_empty(v)}
        task_dict = dict(
            name    = parser.__name__,
            build   = getattr(parser, '__build__', 0),  # User specified
            rev     = getattr(parser, '__rev__',   0),  # User specified
            preferred_upsert_keys = \
                      getattr(parser, '__preferred_upsert_keys__', None),
            options = parser_options,
            ** amended
        )

        res = collection.find_one(task_dict, {'_id': 1})
        if res:
            self.current_task = res['_id']
            return True, res['_id']

        record_dict = dict(
            ** task_dict,
            doc = parser.__doc__,
            __dev = {
                "package": parser.__package__,
                "file":    parser.__file__,
            },
            created = now
        )
        res = collection.insert_one(record_dict)

        self.current_task = res.inserted_id
        return False, res.inserted_id


    def push_task_record(self, action, **kargs):
        now = datetime.utcnow()

        collection = self.db[self.cname_tasks]

        amended = {k: v for k, v in kargs.items() if not is_empty(v)}

        return collection.update_one(
            filter = { "_id": self.current_task },
            update = {
                "$set": {
                    "updated": now,
                },
                "$push": {
                    "records": dict(
                        action = action,
                        ** amended,
                        created = now
                    )
                },
            }
        )


    # Methods for _files collection

    def reg_file(self, filename, **kargs):
        now = datetime.utcnow()

        collection = self.db[self.cname_files]

        amended = {k: v for k, v in kargs.items() if not is_empty(v)}
        amended = {k: ' => '.join(v) if isinstance(v, (tuple, list)) else v \
                   for k, v in amended.items()}
        file_dict = dict(
            name = filename,
            ** amended
        )

        res = collection.find_one(file_dict, {'_id': 1})
        if res:
            self.current_file = res['_id']
            return True, res['_id']

        record_dict = dict(
            ** file_dict,
            file_info = get_file_info(filename),
            created = now
        )
        res = collection.insert_one(record_dict)

        self.current_file = res.inserted_id
        return False, res.inserted_id


    def file_is_processed(self):
        if not self.current_task:
            return None

        collection = self.db[self.cname_files]
        res = collection.find_one(
            {
                "_id": self.current_file,
                "records._tid": self.current_task,
                "records.action": "completed",
            },
            { "_id": 1 }
        )

        if res:
            return True

        return False


    def push_file_record(self, action, **kargs):
        now = datetime.utcnow()

        collection = self.db[self.cname_files]

        amended = {k: v for k, v in kargs.items() if not is_empty(v)}

        return collection.update_one(
            filter = { "_id": self.current_file },
            update = {
                "$set": {
                    "updated": now,
                },
                "$push": {
                    "records": dict(
                        action = action,
                        _tid = self.current_task,
                        ** amended,
                        created = now
                    )
                },
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
