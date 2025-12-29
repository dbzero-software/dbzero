# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import inspect
import dis
from typing import Callable, Optional
from .dbzero import _wrap_memo_type, set_prefix


def migration(func: Callable) -> Callable:
    """Decorator for marking a function as a migration function"""
    func._db0_migration = None
    return func


def __dyn_prefix(from_type):
    """
    Process a specific init_func and build derived bytecode which
    returns the dynamically applied scope (prefix)
    This operation is only viable for very specific class of object constructors 
    which call db0.set_prefix as the 1st statement
    """
    import types as py_types
    import dis
    import inspect
    import sys
    
    # this is to check if __init__ exists and is not inherited from object
    if not hasattr(from_type, "__init__") or from_type.__init__ == object.__init__:
        return None

    def assemble_code(code, post_code, stop_instr, ret_instr):
        """
        Build a new function by splicing the original code up to the target CALL
        and appending a RETURN from a template. Uses CodeType.replace when
        available for cross-version compatibility (3.8+), with a constructor
        fallback for 3.10/3.11 differences.
        """
        # execute up to the stop instruction + return instruction
        new_bytecode = code.co_code[:stop_instr.offset] + post_code.co_code[ret_instr.offset:]

        # Prefer the safer, version-agnostic replace API (available since 3.8)
        try:
            replaced = code.replace(co_code=new_bytecode)
            return py_types.FunctionType(replaced, from_type.__init__.__globals__)
        except AttributeError:
            # Fallback to manual constructor with version-specific layout
            if sys.version_info >= (3, 11):
                new_code = py_types.CodeType(
                    code.co_argcount,
                    code.co_posonlyargcount,
                    code.co_kwonlyargcount,
                    code.co_nlocals,
                    code.co_stacksize,
                    code.co_flags,
                    new_bytecode,
                    code.co_consts,
                    code.co_names,
                    code.co_varnames,
                    code.co_filename,
                    code.co_name,
                    code.co_qualname,
                    code.co_firstlineno,
                    b'',  # co_linetable
                    code.co_exceptiontable,
                    code.co_freevars,
                    code.co_cellvars,
                )
            else:
                # Python 3.10: note the signature differences and no co_qualname/exceptiontable
                new_code = py_types.CodeType(
                    code.co_argcount,
                    code.co_posonlyargcount,
                    code.co_kwonlyargcount,
                    code.co_nlocals,
                    code.co_stacksize,
                    code.co_flags,
                    new_bytecode,
                    code.co_consts,
                    code.co_names,
                    code.co_varnames,
                    code.co_filename,
                    code.co_name,
                    code.co_firstlineno,
                    b'',  # co_lnotab
                    code.co_freevars,
                    code.co_cellvars,
                )

        return py_types.FunctionType(new_code, from_type.__init__.__globals__)
    
    def template_func(self, prefix = None):
        return set_prefix(self, prefix)
    
    # get index of the first CALL instruction to set_prefix, compatible with 3.8+ through 3.11+
    def find_call_instr(instructions):
        attr_stack = []
        call_instr, ret_instr = None, None
        # process up to 10 instructions
        for instr in instructions[:10]:
            # Python 3.11+ uses LOAD_ATTR, Python 3.8-3.10 uses LOAD_METHOD
            if instr.opname in ["LOAD_GLOBAL", "LOAD_FAST", "LOAD_ATTR", "LOAD_METHOD"]:
                attr_stack.append(instr.argval)
            # this is a likely call to db0.set_prefix
            elif instr.opname in {"CALL", "CALL_FUNCTION", "CALL_METHOD", "CALL_FUNCTION_KW", "CALL_FUNCTION_EX"}:
                if "set_prefix" in attr_stack:
                    return instr
        return None
    
    # get index of the last "RETURN_VALUE" instruction
    def find_ret_instr(instructions):
        for instr in reversed(instructions):
            if instr.opname == "RETURN_VALUE":
                return instr
            
        return None
    
    init_func = from_type.__init__
    # NOTE: return instructions are fetched from the template_func
    call_ = find_call_instr(list(dis.get_instructions(init_func)))
    ret_ = find_ret_instr(list(dis.get_instructions(template_func)))
    # unable to identify the relevant instructions
    if call_ is None or ret_ is None:
        return None
    
    # Extract default values for arguments
    default_args = []
    default_kwargs = {}
    px_map = {}
    signature = inspect.signature(init_func)
    for index, param in enumerate(signature.parameters.values()):
        px_map[param.name] = index
        if param.default is not param.empty:
            default_args.append(param.default)
            default_kwargs[param.name] = param.default
    
    # assemble the callable
    dyn_func = assemble_code(init_func.__code__, template_func.__code__, call_, ret_)
                                    
    # this wrapper is required to populate default arguments
    def dyn_wrapper(*args, **kwargs):
        min_kw = len(default_args) + 1
        for kw in kwargs.keys():
            min_kw = min(min_kw, px_map[kw])
        
        # populate default args and kwargs
        all_kwargs = { **kwargs, **{ k: v for k, v in default_kwargs.items() if k not in kwargs and px_map[k] > min_kw } }            
        return dyn_func(None, *args, *default_args[len(args):min_kw - 1], **all_kwargs)
    
    return dyn_wrapper


def memo(cls: Optional[type] = None, **kwargs) -> type:
    """Transform a standard Python class into a persistent, dbzero-managed object.

    The objects' serialization, storage and lifecycle is handled transparently,
    allowing to interact with it as if it was a regular python object.

    Parameters
    ----------
    singleton : bool, default False
        When True, the decorated class becomes a singleton within its prefix. The first 
        time you instantiate the class, the object is created and persisted. All subsequent 
        calls to the constructor within the same prefix will return the existing instance.
    prefix : str, optional
        Specifies a static prefix for the class and all its instances.
        If not provided, the class uses the current active prefix set by dbzero.open().
    no_default_tags : bool, default False
        If True, dbzero will not automatically add default system tags (such as the class 
        name) to new instances of this class.

    Returns
    -------
    type
        Decorated Memo class.

    Examples
    --------
    Basic persistent class:

    >>> @dbzero.memo
    ... class Task:
    ...     def __init__(self, description):
    ...         self.description = description
    ...         self.completed = False
    >>> 
    >>> # Creates a new persistent object
    >>> task1 = Task("Write documentation")
    >>> # Attribute modifications are automatically persisted
    >>> task1.completed = True

    Singleton pattern:
    
    >>> @dbzero.memo(singleton=True)
    ... class AppSettings:
    ...     def __init__(self, theme="dark"):
    ...         self.theme = theme
    >>> 
    >>> # First call creates the object
    >>> settings1 = AppSettings(theme="light")
    >>> print(settings1.theme)  # "light"
    >>> 
    >>> # Subsequent calls return the *same* object; arguments are ignored
    >>> settings2 = AppSettings(theme="dark")
    >>> print(settings2.theme)  # "light"
    """
    def getfile(cls_):
        # inspect.getfile() can raise TypeError, OSError if cls_ is a built-in class (e.g. defined in a notebook).
        try:
            return inspect.getfile(cls_)
        except (TypeError, OSError):
            return None
            
    def dis_assig(method):
        last_inst = None
        unique_args = set()
        for next_inst in dis.get_instructions(method):
            # value assignment
            if next_inst.opname == "STORE_ATTR":
                # "self" argument put on the stack
                if last_inst and _is_self_load_instruction(last_inst):
                    if next_inst.argval not in unique_args:
                        unique_args.add(next_inst.argval)
                        yield next_inst.argval
            last_inst = next_inst
    
    def _is_self_load_instruction(inst):
        """Check if an instruction loads 'self' onto the stack."""
        if inst.opname == "LOAD_FAST" and inst.arg == 0:
            # Traditional single load of first argument (self)
            return True
        elif inst.opname == "LOAD_FAST_BORROW" and inst.arg == 0:
            # Python 3.14+ borrowed reference load of first argument (self)
            return True
        elif inst.opname == "LOAD_FAST_BORROW_LOAD_FAST_BORROW":
            # Python 3.14+ dual borrowed reference load
            # argval is a tuple, check if 'self' is the second element (target for STORE_ATTR)
            if isinstance(inst.argval, tuple) and len(inst.argval) == 2:
                return inst.argval[1] == 'self'
        return False   
    
    def dis_init_assig(from_type):
        """
        This function disassembles a class constructor and yields names of potentially assignable member variables.
        """        
        # this is to check if __init__ exists and is not inherited from object
        if not hasattr(from_type, "__init__") or from_type.__init__ == object.__init__:
            return

        yield from dis_assig(from_type.__init__)

    def find_migrations(from_type):
        # Get all class attributes
        for name in dir(from_type):
            attr = getattr(from_type, name)
            if callable(attr) and not isinstance(attr, staticmethod) and not isinstance(attr, classmethod):
                if hasattr(attr, '_db0_migration'):
                    yield (attr, list(dis_assig(attr)))
    
    def wrap(cls_):
        # note that we use the __dyn_prefix mechanism only for singletons
        is_singleton = kwargs.get("singleton", False)
        return _wrap_memo_type(cls_, py_file = getfile(cls_), py_init_vars = list(dis_init_assig(cls_)), \
            py_dyn_prefix = __dyn_prefix(cls_) if is_singleton else None, \
            py_migrations = list(find_migrations(cls_)) if is_singleton else None, **kwargs
        )
    
    # See if we're being called as @memo or @memo().
    if cls is None:
        # We're called with parens.
        return wrap
    
    # We're called as @memo without parens.
    return wrap(cls, **kwargs)


@memo(id="Division By Zero/dbzero/MemoBase")
class MemoBase:
    pass
