# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from random import randint
from .memo_test_types import MemoTestClass
    

def get_string(str_len):
    return 'a' * str_len    
    

def rand_array(max_bytes):
    import random
    actual_len = random.randint(1, max_bytes)
    return [1] * (int(actual_len / 8) + 1)

    
def test_cache_size_can_be_updated_at_runtime(db0_fixture):
    cache_0 = db0.get_cache_stats()    
    # create object instances to populate cache
    buf = []
    for _ in range(1000):
        buf.append(MemoTestClass(get_string(1024)))
    cache_1 = db0.get_cache_stats()    
    diff_1 = cache_1["size"] - cache_0["size"]
    # reduce cache size so that only 1/2 of objects can fit
    db0.set_cache_size(512 * 1024)
    cache_2 = db0.get_cache_stats()    
    # make sure cache size / capacity was adjusted with at least 95% accuracy
    assert abs(1.0 - (512 * 1024) / cache_2["size"]) < 0.05
    assert abs(1.0 - cache_2["capacity"] / cache_2["size"]) < 0.05
    
    
def test_base_lock_usage_does_not_exceed_limits(db0_fixture):
    # run this test only in debug mode
    if 'D' in db0.build_flags():
        append_count = 200
        cache_size = 1024 * 1024
        db0.set_cache_size(cache_size)
        usage_1 = db0.get_base_lock_usage()[0]
        for _ in range(append_count):
            _ = MemoTestClass(rand_array(16384))
        db0.clear_cache()
        import gc
        gc.collect()        
        usage_2 = db0.get_base_lock_usage()[0]
        print("usage diff = ", usage_2 - usage_1)
        assert usage_2 - usage_1 < cache_size * 1.5


def test_lang_cache_can_reach_capacity(db0_fixture):
    buf = db0.list()
    # python instances are added to lang cache until it reaches capacity
    initial_capacity = db0.get_lang_cache_stats()["capacity"]    
    for _ in range(initial_capacity * 2):        
        buf.append(MemoTestClass(123))
    # here we call commit to flush from internal buffers (e.g. TagIndex)
    db0.commit()            
    # capacity not changed
    assert db0.get_lang_cache_stats()["capacity"] == initial_capacity
    # capacity might be exceeded due to indeterministic gc collection by Python
    assert db0.get_lang_cache_stats()["size"] < initial_capacity * 2