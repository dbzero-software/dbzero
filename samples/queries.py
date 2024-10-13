import dbzero_ce as db0
from model import *


def all_books():
    return db0.find(Book)

def all_books_of(author):
    return db0.find(Book, author)

def books_by_params(author, **kwargs):
    return db0.find(Book, author)
