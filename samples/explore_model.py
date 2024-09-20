import dbzero_ce as db0
from generate import Book

"""
This script demonstrates techniques of exploring the DBZero prefixes
with the model classes imported into the script.
"""

def values_of(obj, attr_names):
    return [getattr(obj, attr_name) for attr_name in attr_names]
    
def __main__():
    try:
        db0.init()
        db0.open("/division by zero/dbzero/samples")
        for prefix in db0.get_prefixes():
            print(prefix)
            for memo_class in db0.get_memo_classes(prefix):
                attr_names = [attr.name for attr in memo_class.get_attributes()]
                print(attr_names)
                for obj in memo_class.all():                    
                    print(values_of(obj, attr_names))                    
    except Exception as e:
        print(e)
    
if __name__ == "__main__":
    __main__()
