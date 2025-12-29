# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from dataclasses import dataclass
import dbzero as db0
from datetime import datetime

DATA_PX = "scoped-data-px"

@db0.memo
class MemoTestClass:
    def __init__(self, value):
        self.value = value        


@db0.memo
class MemoTestClassWithMethods:
    def __init__(self, value):
        self.value = value
    
    def get_value(self):
        return self.value

    def get_value_as_upper(self):
        return str(self.value).upper()

    def get_value_plus(self, other):
        return self.value + other

    def many_args_method(self, param1, /, param2, *args, param3, **kwargs):
        pass
        
    
@db0.memo(prefix=DATA_PX)
class MemoDataPxClass:
    def __init__(self, value):
        self.value = value        

@db0.memo
class MemoScopedClass:
    def __init__(self, value, prefix=None):
        db0.set_prefix(self, prefix)        
        self.value = value
    

@db0.memo
class MemoTestPxClass:
    def __init__(self, value, prefix=None):        
        db0.set_prefix(self, prefix)
        self.value = value        
    

@db0.memo
class KVTestClass:
    def __init__(self, key, value = None):
        self.key = key
        self.value = value


@db0.memo(singleton=True)
class MemoTestSingleton:
    def __init__(self, value, value_2 = None):
        self.value = value
        if value_2 is not None:
            self.value_2 = value_2        


@db0.memo(singleton=True, prefix=DATA_PX)
class MemoDataPxSingleton:
    def __init__(self, value, value_2 = None):
        self.value = value
        if value_2 is not None:
            self.value_2 = value_2        


@db0.memo(singleton=True)
class MemoScopedSingleton:
    def __init__(self, value = None, value_2 = None, prefix = None):
        db0.set_prefix(self, prefix)
        self.value = value
        if value_2 is not None:
            self.value_2 = value_2        


@db0.memo()
class DynamicDataClass:
    def __init__(self, count, values = None):
        if type(count) is list:
            for i in count:
                setattr(self, f'field_{i}', values[i] if values is not None else i)
        else:
            for i in range(count):
                setattr(self, f'field_{i}', i)


@db0.memo(singleton=True)
class DynamicDataSingleton:
    def __init__(self, count):
        if type(count) is list:
            for i in count:
                setattr(self, f'field_{i}', i)
        else:
            for i in range(count):
                setattr(self, f'field_{i}', i)


@db0.enum(values=["RED", "GREEN", "BLUE"])
class TriColor:
    pass

@db0.enum(values=["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], prefix=DATA_PX)
class MonthTag:
    pass

@db0.memo
class MemoTestThreeParamsClass:
    def __init__(self, value_1, value_2, value_3):  
        self.value_1 = value_1
        self.value_2 = value_2
        self.value_3 = value_3


@db0.memo
class MemoTestCustomLoadClass:
    def __init__(self, value_1, value_2, value_3):  
        self.value_1 = value_1
        self.value_2 = value_2
        self.value_3 = value_3    

    def __load__(self):
        return {
            "v1": self.value_1,
            "v2_v3": {self.value_2: self.value_3}
        }
    
@db0.memo
class MemoTestCustomLoadClassWithParams:
    def __init__(self, value_1, value_2, value_3):  
        self.value_1 = value_1
        self.value_2 = value_2
        self.value_3 = value_3    

    def __load__(self, param=None):
        return {
            "v1": self.value_1,
            "v2_v3": {self.value_2: self.value_3},
            "param": param
        }
    
@db0.memo
class MemoTestClassPropertiesAndImmutables:
    def __init__(self, value):
        self.__value = value
        self.some_param = 5

    @db0.immutable
    def immutable_func(self):
        return self.value

    @property
    def value(self):
        return self.__value

    def normal_method(self):
        pass
    

@db0.memo(singleton=True)
class MemoSingletonWithMigrations:
    def __init__(self, value, value_2 = None):
        self.value = value
        self.__ix_orders = db0.index()
    
    @db0.migration
    def migrate__(self):
        self.__ix_orders = db0.index()
    
    
@db0.memo()
class MemoClassForTags:
    def __init__(self, value):
        self.value = value
    
    
@db0.memo(no_default_tags=True)
class MemoNoDefTags:
    def __init__(self, value):
        self.value = value


@db0.memo
class MemoAnyAttrs:
    def __init__(self, **kwargs):
        # assign from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)


@db0.memo
class MemoTask:
    def __init__(self, type, processor_type, data = None, key = None, parent = None,
                 requirements = None, scheduled_at = None, deadline = None):
        # optional task key (None is allowed)
        self.key = key
        # task type e.g. 'etl'
        self.type = type
        # task specific data dict
        self.data = data
        # task size in task type specific units - e.g. bytes
        self.task_size = 0
        # task creation date and time
        self.created_at = datetime.now()
        # optional deadline (affects task priority)
        self.deadline = deadline
        # optional task execution scheduled date and time
        self.scheduled_at = scheduled_at
        # task status code
        self.status = 0
        # task associated processor type
        self.processor_type = processor_type
        self.runs = []
        self.parent = parent
        self.root = parent.root if parent is not None else None
        self.child_tasks = []
        self.requirements = requirements        
        self.max_retry = None


RANDOM_BYTES = b'DB0'*100000

@db0.memo(no_default_tags=True)
@dataclass
class MemoBlob:
    def __init__(self, size_bytes: int):
        assert size_bytes <= len(RANDOM_BYTES)
        self.data = RANDOM_BYTES[:size_bytes]
