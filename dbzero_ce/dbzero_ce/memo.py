import inspect
import dis
from .dbzero_ce import wrap_memo_type


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
        import types
        import dis
        import inspect
        
        # this is to check if __init__ exists and is not inherited from object
        if not hasattr(from_type, "__init__") or from_type.__init__ == object.__init__:
            return None

        def assemble_code(code, stop_instr, ret_instr):        
            # Now create a new code object
            new_code = types.CodeType(
                code.co_argcount, # Number of arguments
                0, # code.co_posonlyargcount,
                code.co_kwonlyargcount,
                code.co_nlocals,
                code.co_stacksize,
                code.co_flags,
                # execute up to the stop instruction + return instruction
                code.co_code[:stop_instr.offset] + code.co_code[ret_instr.offset:],
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
            return types.FunctionType(new_code, from_type.__init__.__globals__)
        
        # get the indices of first "CALL" and the last "RETURN_VALUE" instructions
        def find_call_ret_instructions(instructions):
            attr_stack = []
            call_instr, ret_instr = None, None
            # process up to 10 instructions
            for instr in instructions[:10]:
                if instr.opname in ["LOAD_GLOBAL", "LOAD_FAST", "LOAD_ATTR"]:
                    attr_stack.append(instr.argval)
                # this is a likely call to db0.set_prefix
                elif instr.opname == "CALL":                    
                    if "set_prefix" in attr_stack:
                        call_instr = instr
                    break
            
            for instr in reversed(instructions):
                if instr.opname == "RETURN_VALUE":
                    ret_instr = instr
                    break
            
            return call_instr, ret_instr
        
        init_func = from_type.__init__
        call_, ret_ = find_call_ret_instructions(list(dis.get_instructions(init_func)))
        # unable to identify the relevant instructions
        if call_ is None or ret_ is None:
            return None
        
        default_args = []
        default_kwargs = {}
        px_map = {}
        signature = inspect.signature(init_func)
        for index, param in enumerate(signature.parameters.values()):
            px_map[param.name] = index
            if param.default is not param.empty:
                default_args.append(param.default)
                default_kwargs[param.name] = param.default
        
        dyn_func = assemble_code(init_func.__code__, call_, ret_)
        # this wrapper is required to populate default arguments
        def dyn_wrapper(*args, **kwargs):
            min_kw = len(default_args) + 1
            for kw in kwargs.keys():
                min_kw = min(min_kw, px_map[kw])
            
            # populate default args and kwargs
            all_kwargs = { **kwargs, **{ k: v for k, v in default_kwargs.items() if k not in kwargs and px_map[k] > min_kw } }
            return dyn_func(None, *args, *default_args[len(args):min_kw - 1], **all_kwargs)
        
        return dyn_wrapper
    
    def dis_init_assig(from_type):
        # this is to check if __init__ exists and is not inherited from object
        if not hasattr(from_type, "__init__") or from_type.__init__ == object.__init__:
            return

        """
        This function disassembles a class constructor and yields names of potentially assignable member variables.
        """        
        last_inst = None
        for next_inst in dis.get_instructions(from_type.__init__):
            # value assignment
            if next_inst.opname == "STORE_ATTR":
                # "self" argument put on the stack
                if last_inst is not None and last_inst.opname == "LOAD_FAST" and last_inst.arg == 0:
                    yield next_inst.argval
            last_inst = next_inst
    
    def wrap(cls_):
        # note that we use the dyn_prefix mechanism only for singletons
        is_singleton = kwargs.get("singleton", False)
        return wrap_memo_type(cls_, py_file = getfile(cls_), py_init_vars = list(dis_init_assig(cls_)), 
            py_dyn_prefix = dyn_prefix(cls_) if is_singleton else None, **kwargs)
    
    # See if we're being called as @memo or @memo().
    if cls is None:
        # We're called with parens.
        return wrap
    
    # We're called as @memo without parens.
    return wrap(cls, **kwargs)


@memo(id="Division By Zero/dbzero_ce/MemoBase")
class MemoBase:
    pass
