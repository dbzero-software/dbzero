import dbzero_ce as db0
import argparse

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
    return [safe_attr(obj, attr_name) for attr_name in attr_names]
    
        
def __main__():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default=None, type=str, help="Location of dbzero files")
    args = parser.parse_args()
    try:
        db0.init(path=args.path)
        for prefix in db0.get_prefixes():
            # open prefix to make it the default one
            db0.open(prefix.name, "r")
            for memo_class in db0.get_memo_classes(prefix):
                # methods not available in the context-free model
                attr_names = [attr.name for attr in memo_class.get_attributes()]                                
                for obj in memo_class.all():
                    print(values_of(obj, attr_names))
    except Exception as e:
        print(e)
    db0.close()
    
    
if __name__ == "__main__":
    __main__()
        