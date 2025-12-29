# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
import inspect
from dbzero import memo
from model import *


"""
A sample script to generate data using dbzero
"""
    
def __main__():
    db0.init()
    db0.open("/division by zero/dbzero/samples")
    for data in [("The Catcher in the Rye", "J.D. Salinger", 1951), 
             ("The Great Gatsby", "F. Scott Fitzgerald", 1925),
             ("The Lord of the Rings", "J.R.R. Tolkien", 1954)]:
        book = Book(*data)
    db0.commit()
    db0.close()
    
if __name__ == "__main__":
    __main__()
