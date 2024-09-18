from collections import namedtuple
import dbzero_ce as db0
from .dbzero_ce import get_raw_prefixes, get_raw_memo_classes, get_raw_attributes


PrefixMetaData = namedtuple("PrefixMetaData", ["name", "uuid"])
AttributeInfo = namedtuple("AttributeInfo", ["name"])

class MemoMetaClass:
    def __init__(self, name, module, type_uuid, is_singleton=False, instance_uuid=None):
        self.__name = name
        self.__module = module
        self.__type_uuid = type_uuid
        self.__is_singleton = is_singleton
        self.__instance_uuid = instance_uuid
    
    @property
    def name(self):
        return self.__name
    
    @property
    def module(self):
        return self.__module
    
    @property
    def type_uuid(self):
        return self.__type_uuid
    
    def get_type(self):
        return db0.fetch(self.__type_uuid)
        
    @property
    def is_singleton(self):
        return self.__is_singleton
    
    @property
    def instance_uuid(self):
        return self.__instance_uuid
    
    def get_instance(self):
        return db0.fetch(self.__instance_uuid)
    
    def get_attributes(self):
        for attribute in get_raw_attributes(self.__type_uuid):
            yield AttributeInfo(attribute[0])
    
    def get_methods(self):
        raise NotImplementedError("Not implemented yet")
    
    def all(self):
        return db0.find(self.get_type())
    
    def __str__(self):
        return f"{self.__module}.{self.__name} ({self.__type_uuid})"
    
    def __repr__(self):
        return f"{self.__module}.{self.__name} ({self.__type_uuid})"
    
    
def get_prefixes():
    for prefix in get_raw_prefixes():
        yield PrefixMetaData(*prefix)


def get_memo_classes(prefix: PrefixMetaData):
    for memo_class in get_raw_memo_classes(prefix.name, prefix.uuid):
        yield MemoMetaClass(*memo_class)
