# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from random import randint
from .memo_test_types import MemoTestClass, MemoTestSingleton
import random
import string
    

def get_string(str_len):
    return 'a' * str_len


@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 4 * 1024 * 1024}], indirect=True)
def test_new_slab_is_created_when_insufficient_remaining_capacity(db0_slab_size):
    buf = []
    buf.append(MemoTestClass([0] * int(2 * 1024 * 1024 / 8)))
    slab_count_1 = len(db0.get_slab_metrics())
    buf.append(MemoTestClass([0] * int(2 * 1024 * 1024 / 8)))
    slab_count_2 = len(db0.get_slab_metrics())
    assert slab_count_2 == slab_count_1 + 1


# FIXME: failing test
# @pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1 * 1024 * 1024}], indirect=True)
# def test_new_slab_not_created_until_insufficient_capacity(db0_slab_size):
#     """
#     Note: test failing due to deferred 'free' operations in MetaAllocator  from inside of v_bvector
#     which cause excess storage utilization
#     """
#     buf = []
#     capacity = sum(slab["remaining_capacity"] for slab in db0.get_slab_metrics().values())
#     slab_count_1 = len(db0.get_slab_metrics())
#     total_size = 0
#     unit_size = int(1024 / 8)
#     while total_size < capacity * 0.8:
#         buf.append(MemoTestClass([0] * unit_size))
#         total_size += unit_size * 8
#     slab_count_2 = len(db0.get_slab_metrics())
#     assert slab_count_2 == slab_count_1


# FIXME: failing test
# @pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1 * 1024 * 1024}], indirect=True)
# def test_slab_space_utilization_with_random_allocs(db0_slab_size):
#     """
#     Note: test failing due to deferred 'free' operations in MetaAllocator from inside of v_bvector
#     which cause excess storage utilization
#     """

#     """
#     Note: due to heuristinc nature this test may ocassionally fail,
#     in such case try reducing the validated fill ratio (i.e. size / capacity proportion)
#     """
#     buf = []
#     capacity = sum(slab["remaining_capacity"] for slab in db0.get_slab_metrics().values())
#     slab_count_1 = len(db0.get_slab_metrics())
#     total_size = 0
#     while total_size < capacity * 0.5:
#         unit_size = randint(1, 2048)
#         buf.append(MemoTestClass([0] * unit_size))
#         total_size += unit_size * 8
#     slab_count_2 = len(db0.get_slab_metrics())
#     assert slab_count_2 == slab_count_1


@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1 * 1024 * 1024}], indirect=True)
def test_slab_space_utilization_with_large_allocs(db0_slab_size):
    """
    Note: due to heuristinc nature this test may ocassionally fail,
    in such case try reducing the validated fill ratio (i.e. size / capacity proportion)
    """    
    # make 2 large allocations so that the 2 slabs are created
    buf = []
    buf.append(MemoTestClass(get_string(628 * 1024)))    
    buf.append(MemoTestClass(get_string(628 * 1024)))
    slab_count_1 = len(db0.get_slab_metrics())
    # make sure the remaining capacity can be utilized without adding more slabs
    capacity = sum(slab["remaining_capacity"] for slab in db0.get_slab_metrics().values())        
    total_size = 0
    buf = []
    while total_size < capacity * 0.6:
        unit_size = randint(1, 2048)
        buf.append(get_string(unit_size))
        total_size += unit_size
    slab_count_2 = len(db0.get_slab_metrics())
    assert slab_count_2 == slab_count_1


@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1 * 1024 * 1024}], indirect=True)
def test_allocation_larger_than_slab_size_fails(db0_slab_size):    
    with pytest.raises(Exception):
        obj = MemoTestClass(get_string(int(1.2 * 1024 * 1024)))


@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1 << 20}], indirect=True)
def test_allocator_alloc_unit_issue(db0_slab_size):
    """
    Test was failing with: Allocator out of space
    Resolution: 
    """
    root = MemoTestSingleton([])
    buf = root.value
    for _ in range(10000):
        buf.append([])
    
    from .data_for_tests import test_strings, test_ints

    # append to random lists
    count = 0
    for _ in range(50000):
        str = test_strings[count % len(test_strings)]
        buf[test_ints[count % len(test_ints)]].append(MemoTestClass(str))
        count += 1


@pytest.mark.stress_test
@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1 << 20}], indirect=True)
def test_allocator_out_of_space_when_small_slab_size_1(db0_slab_size):
    """
    Test was failing with: Allocator out of space
    Resolution: 
    """
    root = MemoTestSingleton([])
    buf = root.value
    for _ in range(10000):
        buf.append([])
    
    from .data_for_tests import test_strings, test_ints
    
    # append to random lists
    count = 0    
    for _ in range(200000):
        str = test_strings[count % len(test_strings)]
        buf[test_ints[count % len(test_ints)]].append(MemoTestClass(str))
        count += 1


@pytest.mark.stress_test
@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1 << 20}], indirect=True)
def test_allocator_out_of_space_when_small_slab_size_2(db0_slab_size):
    def rand_string(max_len):
        str_len = random.randint(1, max_len)
        return ''.join(random.choice(string.ascii_letters) for i in range(str_len))
    
    root = MemoTestSingleton([])
    buf = root.value
    for _ in range(10000):
        buf.append([])
    
    # append to random lists
    count = 0
    for _ in range(100000):
        str = rand_string(256)
        buf[random.randint(0, len(buf) - 1)].append(MemoTestClass(str))
        count += 1