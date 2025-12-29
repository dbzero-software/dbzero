# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass


def rand_string(max_len):
    import random
    import string
    actual_len = random.randint(1, max_len)
    return ''.join(random.choice(string.ascii_letters) for i in range(actual_len))


def rand_array(max_len):
    import random
    actual_len = random.randint(1, max_len)
    return [1] * (int(actual_len / 8) + 1)


def get_memory_usage():
    import os
    import psutil
    process = psutil.Process(os.getpid())
    return process.memory_info().rss

    
@pytest.mark.stress_test
@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1024 * 1024 * 1024}], indirect=True)
def test_create_large_objects_bitset_issue(db0_slab_size):
    """
    This test was failing on BitsetAllocator:invalid address, 
    the effect is aggravated by the large slab size
    """
    append_count = 1000
    buf = []
    total_bytes = 0
    count = 0
    report_bytes = 1024 * 1024
    for _ in range(append_count):
        buf.append(MemoTestClass(rand_string(16 * 1024 + 8192)))
        total_bytes += len(buf[-1].value)
        count += 1
        if total_bytes > report_bytes:
            print(f"Total bytes: {total_bytes}")
            report_bytes += 1024 * 1024
        if count % 1000 == 0:
            print(f"Objects created: {count}")


@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1024 * 1024 * 1024}], indirect=True)
def test_create_large_objects_cache_recycler_issue_1(db0_slab_size):
    """
    This test was failing on CacheRecycler's::update assert
    the effect is aggravated by low cache size
    """
    append_count = 200
    db0.set_cache_size(1024 * 1024)
    for _ in range(append_count):
        _ = MemoTestClass(rand_array(8 * 1024 + 8192))
    

@pytest.mark.stress_test
@pytest.mark.parametrize("db0_slab_size", 
                         [{"slab_size": 1024 * 1024 * 1024}], indirect=True)
def test_create_and_drop_simple_memo_objects(db0_slab_size):
    """
    Run on valgrind with --tool=massif to detect memory problems
    """
    init_mem_usage = get_memory_usage()
    append_count = 250
    buf = db0.list()
    total_count = 0
    report_count_step = 5000
    report_count = report_count_step
    db0.set_cache_size(1 * 1024 * 1024)
    for _ in range(append_count):
        buf = []
        for _ in range(100):
            buf.append(MemoTestClass(1))            
        del buf
        db0.clear_cache()
        total_count += 100
        if total_count > report_count:
            print(f"Memory usage: {get_memory_usage() - init_mem_usage}")            
            print(f"Base lock usage: {db0.get_base_lock_usage() if 'D' in db0.build_flags() else 'unavailable'}")
            print(f"Cache: {db0.get_cache_stats()}")
            report_count += report_count_step

    
@pytest.mark.stress_test
@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1024 * 1024 * 1024}], indirect=True)
def test_create_large_objects_low_cache(db0_slab_size):
    init_mem_usage = get_memory_usage()
    append_count = 10000    
    buf = db0.list()
    total_bytes = 0
    count = 0
    report_bytes_step = 16 * 1024 * 1024
    report_bytes = report_bytes_step
    # reduce cache to 4MB
    db0.set_cache_size(4 * 1024 * 1024)
    for _ in range(append_count):
        obj = MemoTestClass(rand_array(8 * 1024 + 8192))
        buf.append(obj)
        total_bytes += len(obj.value) * 8
        count += 1
        if total_bytes > report_bytes:
            print(f"Total bytes: {total_bytes}")
            print(f"Memory usage: {get_memory_usage() - init_mem_usage}")
            print(f"Base lock usage: {db0.get_base_lock_usage() if 'D' in db0.build_flags() else 'unavailable'}")
            report_bytes += report_bytes_step
