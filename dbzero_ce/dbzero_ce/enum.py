from .dbzero_ce import make_enum


def enum(cls=None, **kwargs):
    def wrap(cls_):
        return make_enum(cls_, **kwargs)
    
    # See if we're being called as @enum or @enum().
    if cls is None:
        # We're called with parens.
        return wrap

    if isinstance(cls, str):
        # enum called as a regular function.
        return make_enum(cls, **kwargs)
    
    # We're called as @enum without parens.
    return wrap(cls, **kwargs)
