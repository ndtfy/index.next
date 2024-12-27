#!/usr/bin/env python
# coding=utf-8
# Stan 2021-02-27

import os
from datetime import datetime

import pymongo


class Db():
    def __init__(
        self,
        dburi,
        tls_ca_file = None,
        dbname      = 'db1',
        cname_files = '_files',
        **kargs
    ):
        self.dburi       = dburi
        self.tls_ca_file = tls_ca_file
        self.dbname      = dbname
        self.cname_files = cname_files

        self.client = pymongo.MongoClient(
            dburi,
            tlsCAFile = tls_ca_file,
        )
        self.db = self.client[dbname]


    def __getitem__(self, cname):
        return self.db[cname]


    def insert_many(self, collection, record_list, verbose=False, **kargs):
        if kargs:
            record_list = list(map(lambda item: dict(item, **kargs), record_list))

        if verbose:
            print(f"[ { datetime.utcnow() } ]: inserting started ({ len(record_list) } records)...", end=" ")

        res = collection.insert_many(record_list)

        if verbose:
            print(f"inserted: { len(res.inserted_ids) }")

        return res


    def upsert_many(self, collection, record_list, key_list=None, verbose=False, **kargs):
        if kargs:
            record_list = list(map(lambda item: dict(item, **kargs), record_list))

        if key_list:
            upserts = [ pymongo.UpdateOne({k: v for k, v in x.items() if k in key_list}, {'$set': x}, upsert=True) for x in record_list ]

        else:
            upserts = [ pymongo.UpdateOne({k: v for k, v in x.items() if not k.startswith('_')}, {'$set': x}, upsert=True) for x in record_list ]

        if verbose:
            print(f"[ { datetime.utcnow() } ]: upserting started ({ len(record_list) } records)...", end=" ")

        res = collection.bulk_write(upserts)

        if verbose:
            print("deleted: %s / inserted: %s / matched: %s / modified: %s / upserted: %s" % (
                res.deleted_count,
                res.inserted_count,
                res.matched_count,
                res.modified_count,
                res.upserted_count)
            )

        return res


    # Methods linked to files collection/table

    def reg_file(self, filename, **kargs):
        dirname = os.path.dirname(filename)
        basename = os.path.basename(filename)

        collection = self.db[self.cname_files]

        res = collection.find_one(
            {'dir': dirname, 'name': basename},
            {'_id': 1}
        )
        if res:
            return True, res['_id']

        record_dict = dict(
            dir = dirname,
            name = basename,
            created = datetime.utcnow(),
            **kargs
        )
        res = collection.insert_one(record_dict)

        return False, res.inserted_id


    def push_file_record(self, _id, action, **kargs):
        dt = datetime.utcnow()

        collection = self.db[self.cname_files]

        res = collection.update_one(
            { '_id': _id },
            {
                '$set': {
                    'updated': dt,
                },
                '$push': {
                    'records': {
                        'created': dt,
                        'action': action,
                        'data': kargs
                    }
                }
            }
        )

        return res
