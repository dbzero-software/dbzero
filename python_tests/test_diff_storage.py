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
    
# FIXME: pending
# def test_diff_storage_of_updated_wide_lock(db0_fixture):
#     ba_1 = db0.bytearray(array.array('B', [1] * (4096 * 2 + 18)).tobytes())
#     root = MemoTestSingleton(ba_1)
#     db0.commit()
#     # FIXME: log
#     print(db0.get_storage_stats())
#     # update only few bytes
#     for index in [4096, 5723, 8191]:
#         ba_1[index] = 2
#     db0.commit()
#     # FIXME: log
#     print(db0.get_storage_stats())
