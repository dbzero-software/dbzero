# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from random import random
import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass
from dataclasses import dataclass
from .conftest import DB0_DIR
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
    value: int = 0
        
    def __init__(self):
        self.value = random.randint(0, 1000000)
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
    
    
def test_excluding_no_cache_instances_from_P0_cache(db0_fixture):
    buf = db0.list()
    initial_cache_size = db0.get_cache_stats()["P_size"]["P0"]
    for _ in range(100):
        obj = MemoNoCacheClass()        
        buf.append(obj)
    
    gc.collect()        
    final_cache_size = db0.get_cache_stats()["P_size"]["P0"]
    # make sure cache utilization is low
    assert abs(final_cache_size - initial_cache_size) < (350 << 10)


def test_fetching_no_cache_objects(db0_fixture):
    px_name = db0.get_current_prefix().name
    buf = db0.list()
    uuid_list = []
    for _ in range(100):
        obj = MemoNoCacheClass()
        buf.append(obj)
        uuid_list.append(db0.uuid(obj))

    db0.close()
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    
    # now fetch objects by uuid
    initial_cache_size = db0.get_cache_stats()["P_size"]["P0"]
    total_len = 0
    for id in uuid_list:
        # NOTE: must fetch with type, otherwise no_cache flag may not be honored
        obj = db0.fetch(MemoNoCacheClass, id)
        # this forces data retrieval
        total_len += len(obj.data)
    
    final_cache_size = db0.get_cache_stats()["P_size"]["P0"]
    # make sure cache utilization is low
    assert abs(final_cache_size - initial_cache_size) < (300 << 10)


def test_find_no_cache_objects(db0_fixture):
    px_name = db0.get_current_prefix().name
    buf = db0.list()    
    for _ in range(100):
        obj = MemoNoCacheClass()
        buf.append(obj)        

    db0.close()
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    
    # now retrieve objects using db0.find
    initial_cache_size = db0.get_cache_stats()["P_size"]["P0"]
    total_len = 0
    for obj in db0.find(MemoNoCacheClass):
        # this forces data retrieval (but not caching)
        total_len += len(obj.data)
    
    assert total_len > 0
    final_cache_size = db0.get_cache_stats()["P_size"]["P0"]
    # make sure cache utilization is low
    assert abs(final_cache_size - initial_cache_size) < (300 << 10)


def test_fetching_no_cache_objects(db0_fixture):
    px_name = db0.get_current_prefix().name
    buf = db0.list()
    uuid_list = []
    for _ in range(100):
        obj = MemoNoCacheClass()
        buf.append(obj)
        uuid_list.append(db0.uuid(obj))

    db0.close()
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    
    # now fetch objects by uuid
    initial_cache_size = db0.get_cache_stats()["P_size"]["P0"]
    total_len = 0
    for id in uuid_list:
        # NOTE: must fetch with type, otherwise no_cache flag may not be honored
        obj = db0.fetch(MemoNoCacheClass, id)
        # this forces data retrieval
        total_len += len(obj.data)
    
    final_cache_size = db0.get_cache_stats()["P_size"]["P0"]
    # make sure cache utilization is low
    assert abs(final_cache_size - initial_cache_size) < (300 << 10)


def test_find_no_cache_objects(db0_fixture):
    px_name = db0.get_current_prefix().name
    buf = db0.list()    
    for _ in range(100):
        obj = MemoNoCacheClass()
        buf.append(obj)        

    db0.close()
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    
    # now retrieve objects using db0.find
    initial_cache_size = db0.get_cache_stats()["P_size"]["P0"]
    total_len = 0
    for obj in db0.find(MemoNoCacheClass):
        # this forces data retrieval (but not caching)
        total_len += len(obj.data)
    
    assert total_len > 0
    final_cache_size = db0.get_cache_stats()["P_size"]["P0"]
    # make sure cache utilization is low
    assert abs(final_cache_size - initial_cache_size) < (350 << 10)
