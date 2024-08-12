import pytest
import dbzero_ce as db0
from random import randint
from .memo_test_types import MemoTestClass
    

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


@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1 * 1024 * 1024}], indirect=True)
def test_new_slab_not_created_until_sufficient_capacity(db0_slab_size):
    buf = []
    capacity = sum(slab["remaining_capacity"] for slab in db0.get_slab_metrics().values())
    slab_count_1 = len(db0.get_slab_metrics())
    total_size = 0
    unit_size = int(1024 / 8)
    while total_size < capacity * 0.8:
        buf.append(MemoTestClass([0] * unit_size))
        total_size += unit_size * 8
    slab_count_2 = len(db0.get_slab_metrics())
    assert slab_count_2 == slab_count_1


@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1 * 1024 * 1024}], indirect=True)
def test_slab_space_utilization_with_random_allocs(db0_slab_size):
    """
    Note: due to heuristinc nature this test may ocassionally fail,
    in such case try reducing the validated fill ratio (i.e. size / capacity proportion)
    """
    buf = []
    capacity = sum(slab["remaining_capacity"] for slab in db0.get_slab_metrics().values())
    slab_count_1 = len(db0.get_slab_metrics())
    total_size = 0
    while total_size < capacity * 0.5:
        unit_size = randint(1, 2048)
        buf.append(MemoTestClass([0] * unit_size))
        total_size += unit_size * 8
    slab_count_2 = len(db0.get_slab_metrics())
    assert slab_count_2 == slab_count_1


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
