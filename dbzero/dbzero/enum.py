from .dbzero import _make_enum


def enum(cls=None, *args, **kwargs):
    def wrap(cls_):
        return _make_enum(cls_, **kwargs)
    
    # See if we're being called as @enum or @enum().
    if cls is None:
        # We're called with parens.
        return wrap

    if isinstance(cls, str):
        # enum called as a regular function.
        return _make_enum(cls, *args, **kwargs)
    
    # We're called as @enum without parens.
    return wrap(cls, **kwargs)
