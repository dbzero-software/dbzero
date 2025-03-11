from collections import namedtuple
from enum import Enum
import itertools
from typing import Iterable, Tuple
import dbzero_ce as db0
import inspect
import importlib
import importlib.util
import os
import sys

from .decorators import check_params_not_equal
from .storage_api import PrefixMetaData
from .dbzero_ce import get_raw_memo_classes


AttributeInfo = namedtuple("AttributeInfo", ["name"])
MethodInfo = namedtuple("MethodInfo", ["name", "signature"])

class MemoMetaClass:
    def __init__(self, name, module, class_uuid, is_singleton=False, instance_uuid=None):
        self.__name = name
        self.__module = module
        self.__class_uuid = class_uuid
        self.__is_singleton = is_singleton
        self.__instance_uuid = instance_uuid
        self.__cls = None
    
    @property
    def name(self):
        return self.__name
    
    @property
    def module(self):
        return self.__module
    
    @property
    def class_uuid(self):
        return self.__class_uuid
    
    def get_class(self):
        if self.__cls is None:
            self.__cls = db0.fetch(self.__class_uuid)
        return self.__cls
    
    def get_type(self):
        return self.get_class().type()
    
    @property
    def is_singleton(self):
        return self.__is_singleton
    
    @property
    def instance_uuid(self):
        return self.__instance_uuid
    
    def get_instance(self):
        """
        Get the associated singleton instance of this class.
        """
        return db0.fetch(self.__instance_uuid)
    
    def get_attributes(self):
        """
        get_attributes works for known and unknown types
        """
        for attr in self.get_class().get_attributes():
            yield AttributeInfo(attr[0])
    
    def get_methods(self):
        """
        get_methods works for known types only
        """
        def is_private(name):
            return name.startswith("_")
        
        for attr_name in dir(self.get_type()):
            attr = getattr(self.get_type(), attr_name)
            if callable(attr) and not isinstance(attr, staticmethod) and not isinstance(attr, classmethod) \
                and not is_private(attr_name):                
                yield MethodInfo(attr_name, inspect.signature(attr))
    
    def all(self, snapshot=None, as_memo_base=False):
        if not as_memo_base and self.get_class().is_known_type():
            if snapshot is not None:
                return snapshot.find(self.get_class().type())
            else:
                return db0.find(self.get_class().type())
        
        # fall back to the base class if the actual model class is not imported
        if snapshot is not None:
            return snapshot.find(db0.MemoBase, self.get_class())
        else:
            return db0.find(db0.MemoBase, self.get_class())
    
    def get_instance_count(self):
        return db0.getrefcount(self.get_class())
    
    def __str__(self):
        return f"{self.__module}.{self.__name} ({self.__class_uuid})"
    
    def __repr__(self):
        return f"{self.__module}.{self.__name} ({self.__class_uuid})"
    
    def __eq__(self, value):
        return self.__class_uuid == value.__class_uuid
    

def get_memo_classes(prefix: PrefixMetaData = None):
    for memo_class in (get_raw_memo_classes(prefix.name, prefix.uuid) if prefix is not None else get_raw_memo_classes()):
        yield MemoMetaClass(*memo_class)


def get_memo_class(class_uuid):
    return MemoMetaClass(*db0.fetch(class_uuid).type_info())

    
class Query:
    def __init__(self, function_obj, name, params, has_kwargs):
        self.__function_obj = function_obj
        self.__name = name
        self.__params = params
        self.__has_kwargs = has_kwargs

    @property
    def name(self):
        return self.__name
    
    @property
    def params(self):
        return self.__params
    
    @property
    def has_kwargs(self):
        return self.__has_kwargs
    
    def has_params(self):
        return len(self.__params) > 0 or self.__has_kwargs
    
    def execute(self, *args, **kwargs):
        return self.__function_obj(*args, **kwargs)


def __import_from_file(file_path):    
    if not os.path.isfile(file_path) and os.path.isfile(file_path + ".py"):
        file_path = file_path + ".py"
    
    # register parent modules
    path = file_path
    has_module = []
    while path != "":
        path, tail = os.path.split(path)
        has_module.append(os.path.isfile(os.path.join(path, "__init__.py")))
        if path not in sys.path and len(has_module) > 1 and has_module[-2]:
            sys.path.append(path)
        if not tail:
            break
    
    file_name = os.path.basename(file_path)
    module_name, _ = os.path.splitext(file_name)
    # Load the module from the file path
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
    
    
def __import_module(module_or_file_name):
    try:
        return importlib.import_module(module_or_file_name)
    except Exception:
        return __import_from_file(module_or_file_name)
    
    
def import_model(module_or_file_name):
    __import_module(module_or_file_name)
    
    
def get_queries(*module_names):
    # Dynamically import modules
    for name in module_names:
        module = __import_module(name)
        # Get all the functions from the module
        functions = inspect.getmembers(module, inspect.isfunction)
        # Iterate through each function and print its name with parameters
        for function_name, function_obj in functions:
            signature = inspect.signature(function_obj)
            has_kwargs = any(
                param.kind == inspect.Parameter.VAR_KEYWORD
                for param in signature.parameters.values())
            params = [param.name for param in signature.parameters.values() if param.kind != inspect.Parameter.VAR_KEYWORD]
            yield Query(function_obj, function_name, params, has_kwargs)


def is_private(name):
        return name.startswith("_")

def get_methods(obj):
    """
    get_methods of a given memo object
    """
    _type = db0.get_type(obj)
    for attr_name in dir(_type):
        attr = getattr(_type, attr_name)
        if callable(attr) and not isinstance(attr, staticmethod) and not isinstance(attr, classmethod) \
            and not is_private(attr_name):                
            yield MethodInfo(attr_name, inspect.signature(attr))



def get_properties(obj):
    """
    get_properties works for known and unknown types
    """
    _type = db0.get_type(obj)

    attributes = []
    if db0.is_memo(obj):
        attributes = list(attr[0] for attr in db0.get_attributes(_type))
    else:
        attributes = list(vars(obj).keys())
    for attr_name in itertools.chain(attributes, dir(_type)):
        if is_private(attr_name):
            continue
        attr = getattr(obj, attr_name)
        is_callable = callable(attr)
        if not is_callable or db0.is_immutable_parameter(attr):
            yield attr_name, is_callable
            continue
        if isinstance(attr, property):
            params = inspect.getfullargspec(attr)
            if check_params_not_equal(params, 0):
                continue
            yield attr_name, True           

class CallableType(Enum):
    PROPERTY = 1
    QUERY = 2
    ACTION = 3
    MUTATOR = 4

def get_callables(obj: object, include_properties = False) -> Iterable[Tuple[str, CallableType]]:
    _type = db0.get_type(obj)

    callable_parameters = [param_name for param_name, is_callable in get_properties(obj) if is_callable]
    for attr_name in dir(_type):
        if is_private(attr_name):
            continue
        attr = getattr(obj, attr_name)
        if not callable(attr):
            continue
        if db0.is_immutable_query(attr):
            yield attr_name, CallableType.QUERY
            continue
        if attr_name in callable_parameters:
            if include_properties:
                yield attr_name, CallableType.PROPERTY
            continue
        params = inspect.getfullargspec(attr)
        if check_params_not_equal(params, 0):
            yield attr_name, CallableType.ACTION
            continue
        yield attr_name, CallableType.MUTATOR
        continue
        