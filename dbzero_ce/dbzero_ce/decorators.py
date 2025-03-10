import inspect


def immutable(f):
    def wrapper(*args, **kwargs):
        retval = f(*args, **kwargs)
        return retval
    params = inspect.getfullargspec(f)
    if len(params.args) != 0 or params.varargs or params.varkw or params.kwonlyargs:
        raise RuntimeError("Immutable function must have no positional parameters")
    wrapper.__db0_immutable = True
    return wrapper


def fulltext(f):
    def wrapper(*args, **kwargs):
        retval = f(*args, **kwargs)
        return retval
    params = inspect.getfullargspec(f)
    if len(params.args) != 1 or params.varargs or params.varkw or params.kwonlyargs:
        raise RuntimeError("Fulltext function must have exacly one positional parameter")
    wrapper.__db0_fulltext = True
    return wrapper

def is_immutable(f):
    return hasattr(f, '__db0_immutable')

def is_fulltext(f):
    return hasattr(f, '__db0_fulltext')