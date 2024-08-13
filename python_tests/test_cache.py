import pytest
import dbzero_ce as db0
from random import randint
from .memo_test_types import MemoTestClass
    

def get_string(str_len):
    return 'a' * str_len    
    

def test_cache_size_can_be_updated_at_runtime(db0_fixture):
    cache_0 = db0.cache_stats()
    # create object instances to populate cache
    buf = []
    for _ in range(1000):
        buf.append(MemoTestClass(get_string(1024)))
    cache_1 = db0.cache_stats()
    diff_1 = cache_1["size"] - cache_0["size"]
    # reduce cache size so that only 1/2 of objects can fit
    db0.set_cache_size(512 * 1024)
    cache_2 = db0.cache_stats()
    assert abs(1.0 - (512 * 1024) / cache_2["size"]) < 0.05
    assert cache_2["capacity"] == cache_2["size"]