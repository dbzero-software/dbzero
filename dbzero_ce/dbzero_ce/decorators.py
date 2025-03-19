import inspect
import functools

def check_params_not_equal(params, count):
    if 'self' in params.args:
        params.args.remove('self')
    return len(params.args) != count or params.varargs or params.varkw or params.kwonlyargs

def immutable(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        retval = f(*args, **kwargs)
        return retval
    params = inspect.getfullargspec(f)
    # Immutable query if True and immutable parameter if false
    wrapper.__db0_immutable_query = check_params_not_equal(params, 0)
    return wrapper


def fulltext(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        retval = f(*args, **kwargs)
        return retval
    params = inspect.getfullargspec(f)
    if check_params_not_equal(params, 1):
        raise RuntimeError("Fulltext function must have exacly one positional parameter")
    wrapper.__db0_fulltext = True
    return wrapper

def is_immutable(f):
    return hasattr(f, '__db0_immutable_query')

def is_immutable_query(f):
    return is_immutable(f) and f.__db0_immutable_query

def is_immutable_parameter(f):
    return is_immutable(f) and not f.__db0_immutable_query

def is_query(f):
    return hasattr(f, '__db0_immutable_query')

def is_fulltext(f):
    return hasattr(f, '__db0_fulltext')