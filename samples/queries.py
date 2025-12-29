# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
from model import *


def all_books():
    return db0.find(Book)

def all_books_of(author):
    return db0.find(Book, author.lower())

def books_by_params(author, **kwargs):
    return db0.find(Book, author.lower(), **kwargs)
