# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import inspect
import functools
import sys

def check_params_not_equal(params, count):
    if 'self' in params.args:
        params.args.remove('self')
    return len(params.args) != count or params.varargs or params.varkw or params.kwonlyargs

def immutable(f):
    """A deorator to mark a function as not modifying."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        retval = f(*args, **kwargs)
        return retval
    params = inspect.getfullargspec(f)
    # Immutable query if True and immutable parameter if false
    wrapper.__db0_immutable_query = check_params_not_equal(params, 0)
    return wrapper


def fulltext(f):
    """A decorator to mark a function as fulltext query."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        retval = f(*args, **kwargs)
        return retval
    params = inspect.getfullargspec(f)
    if check_params_not_equal(params, 1):
        raise RuntimeError("Fulltext function must have exacly one positional parameter")
    wrapper.__db0_fulltext = True
    return wrapper

def table_view(f, operator=None):
    """The table_view decorator marks a specific function or method as generating a table view. The following properties apply:
        - First result represents the table header
        - All other results (rows) are key-decorated (i.e. return tuples with unique identifiers) - see Cell Editor
        - Optional operator may be defined to allow cell editions (see Cell Operator)
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        retval = f(*args, **kwargs)
        return retval
    # Immutable query if True and immutable parameter if false
    wrapper.__db0_table_view = True
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

def is_table_view(f):
    return hasattr(f, '__db0_table_view')

def _get_function_context(f):
    context = sys.modules[f.__module__]
    path = f.__qualname__.split('.')
    if path:
        if path[-1] == f.__name__:
            path.pop()
        for p in path:
            context = getattr(context, p, None)
            if context is None:
                raise RuntimeError(f'Unresolvable function context: {f.__module__}:{f.__qualname__}')
    return context

def complete_with(action):
    """A decorator for specifying confirmed action."""
    def decorator(f):
        # context = _get_function_context(f)
        # if not hasattr(context, action):
        #     raise TypeError(f'Complete action "{action}" not found in the context')

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)
        wrapper.__db0_complete_with_action = action
        return wrapper

    return decorator

def has_complete_action(f):
    return hasattr(f, '__db0_complete_with_action')

def get_complete_action_name(f):
    return f.__db0_complete_with_action

def get_complete_action(f):
    action = get_complete_action_name(f)

    context = getattr(f, '__self__', None)
    if context is None:
        context = _get_function_context(f)

    return getattr(context, action)
