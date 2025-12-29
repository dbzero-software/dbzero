# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from __future__ import annotations
from collections import namedtuple
from enum import Enum
import itertools
import pathlib
import pkgutil
import typing
import dbzero as db0
import inspect
import importlib
import importlib.util
import os
import sys
from typing import Any, List

from .decorators import check_params_not_equal
from .storage_api import PrefixMetaData
from .dbzero import _get_memo_classes, _get_memo_class


_CallableParams = namedtuple("CallableParams", ["params", "has_args", "has_kwargs"])
def _get_callable_params(parameters):
    params = []
    has_args = False
    has_kwargs = False
    for param in parameters.values():
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            has_args = True
        elif param.kind == inspect.Parameter.VAR_KEYWORD:
            has_kwargs = True
        else:
            params.append(param)

    return _CallableParams(params, has_args, has_kwargs)


class AttributeInfo:
    """Metadata info of Memo class attriute."""

    def __init__(self, name: str, metaclass: MemoMetaClass):
        self.name = name
        self.metaclass = metaclass


class MethodInfo(inspect.Signature):
    """Metadata info of Memo class method."""

    def __init__(self, name: str, signature: inspect.Signature, metaclass: MemoMetaClass):
        super().__init__(signature.parameters.values(), return_annotation=signature.return_annotation)
        self.name = name
        self.metaclass = metaclass
        self.__params = _get_callable_params(self.parameters)

    def get_params(self) -> List[MethodParam]:
        return [MethodParam(param, self) for param in self.__params.params]

    @property
    def has_args(self) -> bool:
        return self.__params.has_args

    @property
    def has_kwargs(self) -> bool:
        return self.__params.has_kwargs


class MethodParam(inspect.Parameter):
    """Metadata info of Memo class method parameter."""

    def __init__(self, param: inspect.Parameter, method: MethodInfo):
        super().__init__(param.name, param.kind, default=param.default, annotation=param.annotation)
        self.method = method


class MemoMetaClass:
    """Memo class metadata info."""

    def __init__(self, name, module, class_uuid, is_singleton=False, instance_uuid=None):
        self.__name = name
        self.__module = module
        self.__class_uuid = class_uuid
        self.__is_singleton = is_singleton
        self.__instance_uuid = instance_uuid
        self.__cls = None
    
    @property
    def name(self) -> str:
        """Memo class name."""
        return self.__name
    
    @property
    def module(self):
        """Module containing memo class."""
        return self.__module
    
    @property
    def class_uuid(self) -> str:
        """UUID of memo class."""
        return self.__class_uuid
    
    def get_class(self) -> Any:
        """Get Memo class object."""
        if self.__cls is None:
            self.__cls = db0.fetch(self.__class_uuid)
        return self.__cls

    def type_exists(self) -> bool:
        """Check if Memo class Python type exist and can be resolved."""
        return self.get_class().type_exists()

    def get_type(self)-> type:
        """Get Memo class Python type."""
        return self.get_class().type()
    
    @property
    def is_singleton(self):
        """Is Memo class a singleton."""
        return self.__is_singleton
    
    @property
    def instance_uuid(self):
        """Memo class singleton object instance UUID."""
        return self.__instance_uuid
    
    def get_instance(self):
        """Get the associated singleton instance of this class."""
        return db0.fetch(self.__instance_uuid)
    
    def get_attributes(self, include_properties=False):
        """Get attribute info of a Memo class."""
        for attr in self.get_class().get_attributes():
            yield AttributeInfo(attr[0], self)

        if include_properties and self.type_exists():
            # Optionally add properties when requested (and available)
            memo_type = self.get_type()
            for attr_name in dir(memo_type):
                if not attr_name.startswith("_") and isinstance(getattr(memo_type, attr_name), property):
                    yield AttributeInfo(attr_name, self)
    
    def get_methods(self):
        """Get Memo class methods of known Python type."""
        def is_private(name):
            return name.startswith("_")
        
        memo_type = self.get_type()
        for attr_name in dir(memo_type):
            attr = getattr(memo_type, attr_name)
            if callable(attr) and not isinstance(attr, staticmethod) and not isinstance(attr, classmethod) \
                and not is_private(attr_name):                
                yield MethodInfo(attr_name, inspect.signature(attr), self)
    
    def get_schema(self):
        """Get Memo class attributes schema."""
        return db0.get_schema(self.get_type())
    
    def all(self, snapshot=None, as_memo_base=False):
        """Find all instances of this Memo class."""

        cls = self.get_class()
        prefix_name = db0.get_prefix_of(cls).name

        if not cls.type_exists():
            as_memo_base = True

        if as_memo_base:
            if snapshot is not None:
                return snapshot.find(db0.MemoBase, cls, prefix=prefix_name)
            else:
                return db0.find(db0.MemoBase, cls, prefix=prefix_name)

        if snapshot is not None:
            return snapshot.find(cls.type(), prefix=prefix_name)
        else:
            return db0.find(cls.type(), prefix=prefix_name)
    
    def get_instance_count(self):
        """Get number of instances of this Memo class."""
        return db0.getrefcount(self.get_class())
    
    def __str__(self):
        return f"{self.__module}.{self.__name} ({self.__class_uuid})"
    
    def __repr__(self):
        return f"{self.__module}.{self.__name} ({self.__class_uuid})"
    
    def __eq__(self, value):
        return self.__class_uuid == value.__class_uuid
        
    
def get_memo_classes(prefix: PrefixMetaData = None):
    """Get metadata info of all Memo classes."""
    if type(prefix) is str:
        # fallback to prefix name
        for memo_class in (_get_memo_classes(prefix) if prefix is not None else _get_memo_classes()):
            yield MemoMetaClass(*memo_class)
    else:
        for memo_class in (_get_memo_classes(prefix.name, prefix.uuid) if prefix is not None else _get_memo_classes()):
            yield MemoMetaClass(*memo_class)


def get_memo_class(arg: str | db0.MemoBase):
    """Get Memo class metadata info from class UUID or of a Memo object instance."""
    type_info = _get_memo_class(arg) if db0.is_memo(arg) else db0.fetch(arg).type_info()    
    return MemoMetaClass(*type_info)


class Query(inspect.Signature):
    """dbzero query function metadata info."""
    def __init__(self, name: str, function_obj: typing.Callable):
        signature = inspect.signature(function_obj)
        super().__init__(signature.parameters.values(), return_annotation=signature.return_annotation)
        self.__name = name
        self.__function_obj = function_obj
        self.__params = _get_callable_params(self.parameters)

    @property
    def name(self):
        """Function name."""
        return self.__name

    @property
    def function_object(self):
        """Function callable."""
        return self.__function_obj

    @property
    def has_kwargs(self):
        """Has **kwargs arguments."""
        return self.__params.has_kwargs

    @property
    def has_params(self):
        """Has any named of kwargs arguments."""
        return len(self.__params.params) > 0 or self.has_kwargs

    def get_params(self):
        """Get query function parameters."""
        return [QueryParam(param, self) for param in self.__params.params]
    
    def execute(self, *args, **kwargs):
        """Execute query function."""
        return self.__function_obj(*args, **kwargs)

class QueryParam(inspect.Parameter):
    """Query function parameter metadata info."""
    def __init__(self, param: inspect.Parameter, query: Query):
        super().__init__(param.name, param.kind, default=param.default, annotation=param.annotation)
        self.query = query


def __import_from_directory(paths, submodule_search_locations):
    # get all files in the directory
    modules = []
    for (dirpath, dirnames, filenames) in os.walk(paths):
        for filename in filenames:
            if filename.endswith(".py") and not filename.startswith("__"):
                modules.append(__import_from_file(os.path.join(dirpath, filename), 
                                                  submodule_search_locations))
    return modules



def __import_from_file(file_path, submodule_search_locations):
    path_obj = pathlib.Path(file_path)
    spec = importlib.util.spec_from_file_location(path_obj.stem, file_path, submodule_search_locations=submodule_search_locations)
    if spec is None:
        raise ImportError(f"Cannot find module {path_obj.stem} in {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[path_obj.stem] = module
    spec.loader.exec_module(module)
    path_obj = pathlib.Path(file_path)
    return importlib.import_module(path_obj.stem)
    

def __import_submodules(package, module):
    if not hasattr(package, "__path__"):
        return
    for loader, module_name, is_pkg in pkgutil.iter_modules(package.__path__):
        full_module_name = f"{package.__name__}.{module_name}"
        submodule = importlib.import_module(full_module_name, module)
        # Optionally inject public names from submodules into global namespace
        for attr in dir(submodule):
            if not attr.startswith('_'):
                globals()[attr] = getattr(submodule, attr)

def __import_module(module_or_file_name, package = None):
    try:
        module = importlib.import_module(module_or_file_name, package)
        # Optionally import all public attributes
        sys.modules[module_or_file_name] = module
        __import_submodules(module, package)
        return [module]
    except Exception as ex:
        pass
    submodule_search_locations = None
    if package:
        submodule_search_locations = [path[0] for path in os.walk(package) if not path[0].endswith("__pycache__")]
    
    if os.path.isdir(module_or_file_name):
        return __import_from_directory(module_or_file_name, submodule_search_locations)
    else:
        return [__import_from_file(module_or_file_name, package)]


def import_model(module_or_file_name, package=None):
    """Import dbzero Memo classes from a Python package."""
    return importlib.import_module(module_or_file_name, package)


def get_queries(*module_names):
    """Get dbzero query functions from Python modules."""
    # Dynamically import modules
    for name in module_names:
        module = importlib.import_module(name)
        # Get all the functions from the module
        functions = inspect.getmembers(module, inspect.isfunction)
        for function_name, function_obj in functions:
            yield Query(function_name, function_obj)


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
            yield MethodInfo(attr_name, inspect.signature(attr), _type)


def get_properties(obj):
    """
    get_properties works for known and unknown types
    """
    _type = db0.get_type(obj)

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

def get_callables(obj: object, include_properties = False) -> typing.Iterable[typing.Tuple[str, CallableType]]:
    _type = db0.get_type(obj)

    callable_attrs = [attr_name for attr_name in dir(_type) \
                      if not is_private(attr_name) and callable(getattr(_type, attr_name))]
    
    queries = []
    for attr_name in list(callable_attrs):
        # First, find all 'queries' and all associated 'complete' actions and filter them out
        attr = getattr(obj, attr_name)
        if db0.is_immutable_query(attr):
            queries.append(attr_name)
            callable_attrs.remove(attr_name)
            if db0.has_complete_action(attr):
                try:
                    callable_attrs.remove(db0.get_complete_action_name(attr))
                except ValueError:
                    pass
    
    callable_properties = {param_name for param_name, is_callable in get_properties(obj) if is_callable}

    properties = []
    actions = []
    mutators = []
    for attr_name in callable_attrs:
        if attr_name in callable_properties:
            if include_properties:
                properties.append(attr_name)
            continue

        attr = getattr(obj, attr_name)
        params = inspect.getfullargspec(attr)
        if check_params_not_equal(params, 0):
            actions.append(attr_name)
        else:
            mutators.append(attr_name)

    def _yield_attrs(callable_attrs, attrs_type):
        return ((attr, attrs_type) for attr in callable_attrs)

    return itertools.chain(
        _yield_attrs(properties, CallableType.PROPERTY),
        _yield_attrs(queries, CallableType.QUERY),
        _yield_attrs(actions, CallableType.ACTION),
        _yield_attrs(mutators, CallableType.MUTATOR)
    )
