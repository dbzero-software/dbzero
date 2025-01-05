import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR
import array


def test_diff_storage_cow_data_present(db0_fixture):
    root = MemoTestSingleton([])
    # add objects to root in multiple transactions
    for _ in range(30):
        for i in range(10):
            root.value.append(MemoTestClass(i))        
        # before commit, review the CoW data availability statistics
        stats = db0.get_prefix_stats()
        assert stats['cache']['dirty_dp_total'] == stats['cache']['dirty_dp_cow']
        db0.commit()
    
    
def test_diff_storage_of_updated_wide_lock(db0_fixture):
    ba_1 = db0.bytearray(array.array('B', [1] * (4096 * 2 + 18)).tobytes())
    root = MemoTestSingleton(ba_1)
    db0.commit()
    stats_1 = db0.get_storage_stats()
    # update only few bytes
    for index in [12, 1823, 4096, 5723, 8191, 8192, 8193, 8194, 8195, 8196]:
        ba_1[index] = 2
    db0.commit()
    stats_2 = db0.get_storage_stats()    
    diff_usage = stats_2['page_io_diff_bytes'] - stats_1['page_io_diff_bytes']
    # make sure diff-storage usage is not more that 1DP
    assert diff_usage <= db0.get_prefix_stats()["dp_size"]
    # make sure everything was written using the diff-storage
    assert stats_2['page_io_total_bytes'] - stats_1['page_io_total_bytes'] == diff_usage        
    
    
def test_durability_of_diff_stored_wide_lock(db0_fixture):
    px_name = db0.get_current_prefix().name
    ba_1 = db0.bytearray(array.array('B', [1] * (4096 * 2 + 18)).tobytes())
    ba_1_len = len(ba_1)
    root = MemoTestSingleton(ba_1)
    db0.commit()
    stats_1 = db0.get_storage_stats()
    loc_bytes = [12, 1823, 4096, 5723, 8191, 8192, 8193, 8194, 8195, 8196]
    # update only few bytes
    for index in loc_bytes:
        ba_1[index] = 2
    del ba_1
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    ba_2 = MemoTestSingleton().value    
    assert len(ba_2) == ba_1_len
    for index in range(ba_1_len):
        assert ba_2[index] == 1 if index not in loc_bytes else 2
    