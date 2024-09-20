import inspect
from .dbzero_ce import wrap_memo_type


def memo(cls=None, **kwargs):
    def wrap(cls_):
        return wrap_memo_type(cls_, py_file = inspect.getfile(cls_), **kwargs)
    
    # See if we're being called as @memo or @memo().
    if cls is None:
        # We're called with parens.
        return wrap

    # We're called as @memo without parens.
    return wrap(cls, **kwargs)
