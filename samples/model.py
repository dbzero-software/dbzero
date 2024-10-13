import dbzero_ce as db0

"""
A sample data model
"""

@db0.memo
class Book:
    def __init__(self, title, author, year):
        self.title = title
        self.author = author
        self.year = year
        db0.tags(self).add(author)
    
    def full_desc(self):
        return f"{self.title} by {self.author} ({self.year})"
    
    def author_initials(self):
        return "".join([name[0] for name in self.author.split()])
