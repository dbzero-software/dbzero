# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
import argparse
import itertools

"""
This script demonstrates techniques of exploring the DBZero prefixes without 
any prior knowledge of the underlying class model.
"""

def values_of(obj, attr_names):
    def safe_attr(obj, attr_name):
        try:
            return getattr(obj, attr_name)
        except db0.ClassNotFoundError:
            # cast to MemoBase if a specific model type is not known (e.g. missing import)
            return db0.getattr_as(obj, attr_name, db0.MemoBase)
        except AttributeError as e:
            return None
    return [db0.uuid(obj)] + [safe_attr(obj, attr_name) for attr_name in attr_names]
    

def values_of_memo_base(obj, attr_names):
    def safe_attr(obj, attr_name):
        try:            
            return db0.getattr_as(obj, attr_name, db0.MemoBase)
        except AttributeError as e:
            return None
    return [db0.uuid(obj)] + [safe_attr(obj, attr_name) for attr_name in attr_names]

 
def __main__():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default=None, type=str, help="Location of dbzero files")
    parser.add_argument('--limit', default=None, type=int, help="Limit of rows per collection")
    parser.add_argument('--uuid', default=None, type=str, help="UUID of the object to be shown")
    args = parser.parse_args()
    
    db0.init(dbzero_root=args.path)
    try:
        for prefix in db0.get_prefixes():
            # open prefix to make it the default one
            db0.open(prefix.name, "r")
            for memo_class in db0.get_memo_classes(prefix):
                print(f"--- Prefix: {prefix.name} / # {db0.get_state_num(prefix.name)} ---")
                print(f"Class: {prefix.name}/{memo_class.name}")
                # methods not available in the context-free model
                attr_names = [attr.name for attr in memo_class.get_attributes()]
                is_known_type = memo_class.get_class().is_known_type()
                if args.uuid is not None:
                    print(values_of_memo_base(db0.fetch(db0.MemoBase, args.uuid), attr_names))
                else:
                    for obj in itertools.islice(memo_class.all(), args.limit):
                        if is_known_type:
                            print(values_of(obj, attr_names))
                        else:
                            print(values_of_memo_base(obj, attr_names))
    finally:
        db0.close()
    
    
if __name__ == "__main__":
    __main__()
    