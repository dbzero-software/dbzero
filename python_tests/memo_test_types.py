import dbzero_ce as db0

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
        if prefix is not None:
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
        print("normal method")
    

@db0.memo(singleton=True)
class MemoSingletonWithMigrations:
    def __init__(self, value, value_2 = None):
        self.value = value
        self.__ix_orders = db0.index()
    
    @db0.migration
    def migrate__(self):
        self.__ix_orders = db0.index()
    