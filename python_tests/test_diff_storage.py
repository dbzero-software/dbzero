import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR


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
    