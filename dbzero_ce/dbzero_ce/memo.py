import inspect
from .dbzero_ce import wrap_memo_type


def memo(cls=None, **kwargs):
    def getfile(cls_):
        # inspect.getfile() can raise TypeError if cls_ is a built-in class (e.g. defined in a notebook).
        try:
            return inspect.getfile(cls_)
        except TypeError:
            return None
    
    def wrap(cls_):
        return wrap_memo_type(cls_, py_file = getfile(cls_), **kwargs)
    
    # See if we're being called as @memo or @memo().
    if cls is None:
        # We're called with parens.
        return wrap
    
    # We're called as @memo without parens.
    return wrap(cls, **kwargs)


@memo(id="Division By Zero/dbzero_ce/MemoBase")
class MemoBase:
    pass
