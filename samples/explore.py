import dbzero_ce as db0

"""
This script demonstrates techniques of exploring the DBZero prefixes without 
any prior knowledge of the underlying class model.
"""

def __main__():
    db0.init()
    db0.open("/division by zero/dbzero/samples")
    for prefix in db0.get_prefixes():
        print(prefix)
        for memo_class in db0.get_memo_classes(prefix):
            print(memo_class.name)
            for obj in memo_class.all():
                print(obj)
    
if __name__ == "__main__":
    __main__()
        