# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
from model import *

"""
This script demonstrates techniques of exploring the DBZero prefixes
with the model classes imported into the script.
"""

def values_of(obj, attr_names, methods):
    result = [getattr(obj, attr_name) for attr_name in attr_names]
    for method in methods:
        result.append(getattr(obj, method.name)())
    return result
    
def __main__():
    try:
        db0.init()
        for prefix in db0.get_prefixes():
            # open prefix to make it the default one
            db0.open(prefix.name, "r")
            for memo_class in db0.get_memo_classes(prefix):
                attr_names = [attr.name for attr in memo_class.get_attributes()]
                # extract only zero-argument methods (self not counted)                
                methods = list(meth for meth in memo_class.get_methods() if len(meth.parameters) == 1)                
                for obj in memo_class.all():
                    print(values_of(obj, attr_names, methods))
    except Exception as e:
        print(e)
    db0.close()
    
if __name__ == "__main__":
    __main__()
