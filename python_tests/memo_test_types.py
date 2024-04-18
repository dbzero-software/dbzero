import dbzero_ce as db0


@db0.memo
class MemoTestClass:
    def __init__(self, value):
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
