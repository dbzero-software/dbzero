from random import random
import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, TriColor, MemoAnyAttrs
from dataclasses import dataclass
import random
import string
import gc


def rand_string(max_len):
    str_len = random.randint(1, max_len)
    return ''.join(random.choice(string.ascii_letters) for i in range(str_len))


@db0.memo(no_cache=True)
#@db0.memo
@dataclass
class MemoNoCacheClass:
    data: str
    
    def __init__(self):
        self.data = rand_string(12 << 10)
    
        
def test_create_memo_no_cache(db0_fixture):
    obj_1 = MemoNoCacheClass()
    assert obj_1.data is not None


def test_no_cache_instances_removed_from_lang_cache(db0_fixture):
    buf = db0.list()
    for _ in range(100):
        obj = MemoNoCacheClass()
        buf.append(obj)
    
    gc.collect()
    # make sure objects were not cached
    assert db0.get_lang_cache_stats()["size"] < 10        
    
    
def test_excluding_no_cache_instances_from_dbzero_cache(db0_fixture):
    buf = db0.list()
    for _ in range(100):
        obj = MemoNoCacheClass()
        buf.append(obj)
        print(db0.get_cache_stats())

    gc.collect()
    