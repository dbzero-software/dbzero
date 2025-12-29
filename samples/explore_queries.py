# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
import argparse


def values_of(obj, attr_names):
    return [getattr(obj, attr_name) for attr_name in attr_names]


def print_query_rows(rows):
    columns = None
    for row in rows:
        if type(row) is tuple:
            print(list(row))
        else:
            if not columns:
                columns = [attr[0] for attr in db0.get_attributes(type(row))]
            print(values_of(row, columns))
    
    
def print_query_results(query_result):
    if type(query_result) is dict:
        def to_list(element):
            if isinstance(element, (list, tuple)):
                return list(element)
            return [element]

        def as_rows():
            for key, value in query_result.items():
                yield (*to_list(key), value)
        print_query_rows(as_rows())
    else:
        print_query_rows(query_result)
    

def parse_unknown_args(args):
    result = {}
    for arg in args:
        if arg.startswith('--'):
            if '=' in arg:
                # Split the argument at the '=' for --arg=value format
                key, value = arg.lstrip('--').split('=', 1)
                result[key] = value
            else:
                # For --arg value format, handle the next element as the value
                key = arg.lstrip('--')
                # Check if there is a next argument and if it doesn't start with '--'
                value = None
                if args.index(arg) + 1 < len(args):
                    next_arg = args[args.index(arg) + 1]
                    if not next_arg.startswith('--'):
                        value = next_arg
                result[key] = value        
    return result


def __main__():
    """
    Usage examples:
    python -m generate
    python -m explore_queries --query queries.all_books
    python -m explore_queries --path /path/to/dbzero/files --query queries.all_books_of --author "Salinger"
    python -m explore_queries --path="/src/zorch/app-data" --query="/src/zorch/zorch/queries.pipeline_progress"
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default=None, type=str, help="Location of dbzero files")
    parser.add_argument('--query', type=str, help="Dot delimited module containing queries and query name (e.g. my_module.my_query)")
    args, extra_args = parser.parse_known_args()
    
    # parse unknown (query) args
    query_args = parse_unknown_args(extra_args)
    if len(query_args) > 0:
        print(f"Query args: {query_args}")
    
    try:
        db0.init(dbzero_root=args.path)
        db0.init_fast_query(prefix="fq_cache")
        # open fq_cache as the initially default prefix
        db0.open("fq_cache")
        # open/create the FastQueryCache singleton to be able to run fq-based queries
        _ = db0.FastQueryCache()
        # open all available prefixes next
        for prefix in db0.get_prefixes():
            db0.open(prefix.name, "r")
        
        query_module, query_name = args.query.split(".")
        queries = { query.name: query for query in db0.get_queries(query_module) }
        if query_name not in queries:
            raise ValueError(f"Query {query_name} not found in module {query_module}")        
        query = queries[query_name]
        print(f"--- Query {query.name} ---")
        print_query_results(query.execute(**query_args))
    finally:
        db0.close()
    
    
if __name__ == "__main__":
    __main__()
    
