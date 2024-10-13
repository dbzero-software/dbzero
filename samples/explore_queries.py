import dbzero_ce as db0
import argparse
import importlib
import inspect


def values_of(obj, attr_names):
    return [getattr(obj, attr_name) for attr_name in attr_names]


def print_query_results(query):
    columns = None
    for row in query.execute():
        if not columns:
            columns = [attr[0] for attr in db0.get_attributes(type(row))]
        print(values_of(row, columns))
    
    
def __main__():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default=None, type=str, help="Location of dbzero files")
    parser.add_argument('--queries', type=str, help="Module containing queries")
    args = parser.parse_args()
    try:
        db0.init(path=args.path)
        # open all available prefixes first
        for prefix in db0.get_prefixes():
            db0.open(prefix.name, "r")
        for query in db0.get_queries(args.queries):
            print(f"--- Query {query.name} ---")
            print_query_results(query)
    except Exception as e:
        print(e)
    db0.close()

    
if __name__ == "__main__":
    __main__()
    
