from collections import namedtuple
import dbzero_ce as db0
import inspect
from .storage_api import PrefixMetaData
from .dbzero_ce import get_raw_memo_classes, get_raw_attributes


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
        
    @property
    def is_singleton(self):
        return self.__is_singleton
    
    @property
    def instance_uuid(self):
        return self.__instance_uuid
    
    def get_instance(self):
        return db0.fetch(self.__instance_uuid)
    
    def get_attributes(self):
        for attribute in get_raw_attributes(self.__class_uuid):
            yield AttributeInfo(attribute[0])
    
    def get_methods(self):
        def is_private(name):
            return name.startswith("_")
        
        for attr_name in dir(self.get_class()):
            attr = getattr(self.get_class(), attr_name)
            if callable(attr) and not isinstance(attr, staticmethod) and not isinstance(attr, classmethod) \
                and not is_private(attr_name):                
                yield MethodInfo(attr_name, inspect.signature(attr))
    
    def all(self):
        return db0.find(self.get_class())
    
    def __str__(self):
        return f"{self.__module}.{self.__name} ({self.__class_uuid})"
    
    def __repr__(self):
        return f"{self.__module}.{self.__name} ({self.__class_uuid})"
    

def get_memo_classes(prefix: PrefixMetaData = None):
    for memo_class in (get_raw_memo_classes(prefix.name, prefix.uuid) if prefix is not None else get_raw_memo_classes()):
        yield MemoMetaClass(*memo_class)
