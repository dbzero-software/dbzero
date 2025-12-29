# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0

"""
A sample data model
"""

@db0.memo
class Book:
    def __init__(self, title, author, year):
        self.title = title
        self.author = author
        self.year = year
        db0.tags(self).add([token.lower().rstrip(".") for token in author.split(" ") if len(token) > 2])
    
    def full_desc(self):
        return f"{self.title} by {self.author} ({self.year})"
    
    def author_initials(self):
        return "".join([name[0] for name in self.author.split()])
