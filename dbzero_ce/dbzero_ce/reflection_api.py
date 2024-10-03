from collections import namedtuple
import dbzero_ce as db0
import inspect
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
    
    def all(self):
        try:
            py_type = self.get_class().type()
        except:
            # fall back to the base class if the actual model class is not imported
            return db0.find(db0.MemoBase, self.get_class())
        return db0.find(py_type)
    
    def get_instance_count(self):
        return db0.getrefcount(self.get_class())
    
    def __str__(self):
        return f"{self.__module}.{self.__name} ({self.__class_uuid})"
    
    def __repr__(self):
        return f"{self.__module}.{self.__name} ({self.__class_uuid})"
    

def get_memo_classes(prefix: PrefixMetaData = None):
    for memo_class in (get_raw_memo_classes(prefix.name, prefix.uuid) if prefix is not None else get_raw_memo_classes()):
        yield MemoMetaClass(*memo_class)
