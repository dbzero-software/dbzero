import dbzero_ce as db0
import inspect
from dbzero_ce import memo

"""
A sample script to generate data using DBZero
"""

@memo
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
