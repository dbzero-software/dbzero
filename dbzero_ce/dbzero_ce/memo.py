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
        return wrap_memo_type(cls_, py_file = getfile(cls_), py_init_vars = list(dis_init_assig(cls_)), **kwargs)
    
    # See if we're being called as @memo or @memo().
    if cls is None:
        # We're called with parens.
        return wrap
    
    # We're called as @memo without parens.
    return wrap(cls, **kwargs)


@memo(id="Division By Zero/dbzero_ce/MemoBase")
class MemoBase:
    pass
