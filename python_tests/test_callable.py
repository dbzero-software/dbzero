import builtins
import pytest
import math
from functools import partial
from dbzero import db0
from .memo_test_types import MemoTestClass, func as func_from_other_module
from .conftest import DB0_DIR

def simple_funcion(x):
    return x + 1

def some_func(x):
    return x - 1

multiplier = 10
def closure_func(x):
    return x * multiplier

def fibonacci(n):
    if n <= 1:
        return n
    return fibonacci(n-1) + fibonacci(n-2)

def generator_func(n):
    for i in range(n):
        yield i

def annotated_func(x: int) -> int:
    return x + 1

def test_can_store_callable_as_member(db0_fixture):
    obj = MemoTestClass(simple_funcion)
    value = obj.value(1)
    assert value == 2
    
def test_can_store_callable_from_other_module(db0_fixture):
    obj = MemoTestClass(func_from_other_module)
    value = obj.value(3)
    assert value == 6

def test_can_store_callable_in_singleton(db0_fixture):
    @db0.memo(singleton=True)
    class SingletonWithCallable:
        def __init__(self, func):
            self.func = func

    obj = SingletonWithCallable(func_from_other_module)
    value = obj.func(4)
    assert value == 8
    prefix_name = db0.get_prefix_of(obj).name
    db0.commit()
    db0.close()
    
    # reopen and check again
    db0.init(DB0_DIR)
    db0.open(prefix_name, "rw")
    
    obj = SingletonWithCallable()
    value = obj.func(5)
    assert value == 10

def test_can_store_lambda_as_member(db0_fixture):
    with pytest.raises(Exception):
        obj = MemoTestClass(lambda x: x * 3)

def test_can_store_class_method_as_member(db0_fixture):
    class Helper:
        def method(self, x):
            return x * 2
    
    helper = Helper()
    with pytest.raises(Exception):
        obj = MemoTestClass(helper.method)

def test_callable_with_nested_class(db0_fixture):
    @db0.memo()
    class Container:
        def __init__(self, inner_obj):
            self.inner = inner_obj
    
    inner = MemoTestClass(func_from_other_module)
    outer = Container(inner)
    
    result = outer.inner.value(6)
    assert result == 12

def test_callable_replacement(db0_fixture):
    @db0.memo()
    class DynamicCallable:
        def __init__(self, func):
            self.func = func
        
        def set_func(self, new_func):
            self.func = new_func
    
    obj = DynamicCallable(simple_funcion)
    assert obj.func(5) == 6
    
    obj.set_func(func_from_other_module)
    assert obj.func(5) == 10


def test_builtin_c_function_not_allowed(db0_fixture):
    # A built-in C function should throw an exception
    builtin_func = builtins.len
    
    with pytest.raises(AttributeError) as exc_info:
        obj = MemoTestClass(builtin_func)

def multi_arg_func(x, y, z):
    return x + y + z

def test_callable_with_multiple_args(db0_fixture):
    """Test callable that accepts multiple arguments"""

    obj = MemoTestClass(multi_arg_func)
    value = obj.value(1, 2, 3)
    assert value == 6

def decorator(func):
    def wrapper(x):
        return func(x) * 2
    return wrapper

@decorator
def decorated_func(x):
    return x + 1

def test_callable_with_decorator(db0_fixture):
    """Test storing a decorated function"""
    with pytest.raises(AttributeError):
        obj = MemoTestClass(decorated_func)

def test_callable_with_local_func(db0_fixture):
    """Test storing a decorated function"""
    def some_func(x):
        return x - 1
    with pytest.raises(AttributeError):
        obj = MemoTestClass(some_func)


def test_callable_list_storage(db0_fixture):
    """Test storing multiple callables in a list"""
    @db0.memo()
    class CallableList:
        def __init__(self, funcs):
            self.funcs = funcs
    
    obj = CallableList([simple_funcion, func_from_other_module])
    fnct = obj.funcs[0]
    assert fnct(10) == 11

class Helper:
    @staticmethod
    def static_func(x):
        return x * 3

def test_staticmethod_as_callable(db0_fixture):
    """Test storing a static method as callable"""
    obj = MemoTestClass(Helper.static_func)
    value = obj.value(4)
    assert value == 12


class HelperClassMethod:
    @classmethod
    def class_func(cls, x):
        return x * 4

def test_classmethod_should_fail(db0_fixture):
    """Test that class methods raise an exception"""
    with pytest.raises(AttributeError):
        obj = MemoTestClass(HelperClassMethod.class_func)

def test_callable_with_closure(db0_fixture):
    """Test function with closure variables"""
    obj = MemoTestClass(closure_func)
    value = obj.value(5)
    assert value == 50


def test_callable_recursive_function(db0_fixture):
    """Test storing recursive function"""
    obj = MemoTestClass(fibonacci)
    value = obj.value(6)
    assert value == 8

def test_callable_generator_function(db0_fixture):
    """Test storing generator function"""
    obj = MemoTestClass(generator_func)
    gen = obj.value(5)
    result = list(gen)
    assert result == [0, 1, 2, 3, 4]

def test_callable_partial_function(db0_fixture):
    """Test storing functools.partial object"""
    partial_func = partial(multi_arg_func, 1, 2)
    
    with pytest.raises(AttributeError):
        obj = MemoTestClass(partial_func)

def test_callable_function_with_annotations(db0_fixture):
    """Test storing function with type annotations"""
    obj = MemoTestClass(annotated_func)
    value = obj.value(7)
    assert value == 8