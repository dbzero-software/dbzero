import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestSingleton, MemoTestClass, MemoScopedClass, MemoDataPxClass


def test_select_modified(db0_fixture, memo_tags):
    db0.commit()
    # get the last finalized state number
    last_state_num = db0.get_state_num(finalized = True)
    query = db0.select_modified(db0.find(MemoTestClass), (last_state_num, last_state_num))
    assert len(query) == 10
    
