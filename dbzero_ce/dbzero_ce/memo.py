import inspect
import dis
from .dbzero_ce import wrap_memo_type, set_prefix


def migration(func):
    """
    Decorator for marking a function as a migration function.
    """
    func._db0_migration = None
    return func

    
def memo(cls=None, **kwargs):
    def getfile(cls_):
        # inspect.getfile() can raise TypeError if cls_ is a built-in class (e.g. defined in a notebook).
        try:
            return inspect.getfile(cls_)
        except TypeError:
            return None
        
    def dyn_prefix(from_type):
        """
        Process a specific init_func and build derived bytecode which
        returns the dynamically applied scope (prefix)
        This operation is only viable for very specific class of object constructors 
        which call db0.set_prefix as the 1st statement
        """
        import types as py_types
        import dis
        import inspect
        
        # this is to check if __init__ exists and is not inherited from object
        if not hasattr(from_type, "__init__") or from_type.__init__ == object.__init__:
            return None

        def assemble_code(code, post_code, stop_instr, ret_instr):
            # Now create a new code object
            new_code = py_types.CodeType(
                code.co_argcount, # Number of arguments
                0, # code.co_posonlyargcount,
                code.co_kwonlyargcount,
                code.co_nlocals,
                code.co_stacksize,
                code.co_flags,
                # execute up to the stop instruction + return instruction
                code.co_code[:stop_instr.offset] + post_code.co_code[ret_instr.offset:],
                code.co_consts,
                code.co_names,
                code.co_varnames,
                code.co_filename,
                code.co_name,
                code.co_qualname,
                code.co_firstlineno,
                code.co_linetable,
                code.co_exceptiontable,
                code.co_freevars,
                code.co_cellvars,
            )

            # Create a new function with the modified bytecode
            return py_types.FunctionType(new_code, from_type.__init__.__globals__)
        
        def template_func(self, prefix = None):
            return set_prefix(prefix)
        
        # get index of the first "CALL" instruction
        def find_call_instr(instructions):
            attr_stack = []
            call_instr, ret_instr = None, None
            # process up to 10 instructions
            for instr in instructions[:10]:
                if instr.opname in ["LOAD_GLOBAL", "LOAD_FAST", "LOAD_ATTR"]:
                    attr_stack.append(instr.argval)
                # this is a likely call to db0.set_prefix
                elif instr.opname == "CALL":
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
    
    def dis_assig(method):
        last_inst = None
        unique_args = set()
        for next_inst in dis.get_instructions(method):
            # value assignment
            if next_inst.opname == "STORE_ATTR":
                # "self" argument put on the stack
                if last_inst is not None and last_inst.opname == "LOAD_FAST" and last_inst.arg == 0:
                    if next_inst.argval not in unique_args:
                        unique_args.add(next_inst.argval)
                        yield next_inst.argval
            last_inst = next_inst   
    
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
        # note that we use the dyn_prefix mechanism only for singletons
        is_singleton = kwargs.get("singleton", False)
        return wrap_memo_type(cls_, py_file = getfile(cls_), py_init_vars = list(dis_init_assig(cls_)), \
            py_dyn_prefix = dyn_prefix(cls_) if is_singleton else None, \
            py_migrations = list(find_migrations(cls_)) if is_singleton else None, **kwargs
        )
    
    # See if we're being called as @memo or @memo().
    if cls is None:
        # We're called with parens.
        return wrap
    
    # We're called as @memo without parens.
    return wrap(cls, **kwargs)


@memo(id="Division By Zero/dbzero_ce/MemoBase")
class MemoBase:
    pass
