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
    

def test_memo_no_cache_issue1(db0_fixture):
    """
    Issue: test failing with RuntimeError: BDevStorage::read: page not found: 8, state: 2
    Reason: objects marked with no_cache were not being registered with DirtyCache, added no_dirty_cache flag
    """
    buf = db0.list()
    for _ in range(3):
        obj = MemoNoCacheClass()
        buf.append(obj)
        del obj
    
    
def test_excluding_no_cache_instances_from_dbzero_cache(db0_fixture):
    buf = db0.list()
    initial_cache_size = db0.get_cache_stats()["size"] 
    for _ in range(100):
        obj = MemoNoCacheClass()
        buf.append(obj)
    
    gc.collect()
    final_cache_size = db0.get_cache_stats()["size"]
    # make sure cache utilization is low
    assert abs(final_cache_size - initial_cache_size) < (300 << 10)
    