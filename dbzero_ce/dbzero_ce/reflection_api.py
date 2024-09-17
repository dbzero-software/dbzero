from collections import namedtuple
from .dbzero_ce import get_raw_prefixes, get_raw_memo_classes, get_raw_attributes


PrefixMetaData = namedtuple("PrefixMetaData", ["name", "puuid"])
AttributeInfo = namedtuple("AttributeInfo", ["name"])

class MemoMetaClass:
    def __init__(self, name, module, memo_uuid, is_singleton=False, singleton_uuid=None):
        self.__name = name
        self.__module = module
        self.__memo_uuid = memo_uuid
        self.__is_singleton = is_singleton
        self.__singleton_uuid = singleton_uuid
    
    @property
    def name(self):
        return self.__name
    
    @property
    def module(self):
        return self.__module
    
    @property
    def memo_uuid(self):
        return self.__memo_uuid
    
    @property
    def is_singleton(self):
        return self.__is_singleton
    
    @property
    def singleton_uuid(self):
        return self.__singleton_uuid
    
    def get_attributes(self):
        for attribute in get_raw_attributes(self.__memo_uuid):
            yield AttributeInfo(attribute[0])
    
    def get_methods(self):
        raise NotImplementedError("Not implemented yet")
     
    def __str__(self):
        return f"{self.__module}.{self.__name} ({self.__memo_uuid})"
    
    def __repr__(self):
        return f"{self.__module}.{self.__name} ({self.__memo_uuid})"
    
    
def get_prefixes():
    for prefix in get_raw_prefixes():
        yield PrefixMetaData(*prefix)


def get_memo_classes(*args, **kwargs):
    for memo_class in get_raw_memo_classes(*args, **kwargs):
        yield MemoMetaClass(*memo_class)
