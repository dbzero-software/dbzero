# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, TriColor, MemoTestSingleton


def test_touch_results_in_new_transaction(db0_fixture):
    obj = MemoTestClass(999)
    db0.tags(obj).add("tag1")
    db0.commit()
    state_0 = db0.get_state_num(finalized=True)
    # read operation does not result in a new transaction
    _ = obj.value
    db0.commit()
    state_1 = db0.get_state_num(finalized=True)
    assert state_0 == state_1
    db0.touch(obj)
    db0.commit()
    state_2 = db0.get_state_num(finalized=True)
    # make sure that the state number was changed
    assert state_2 > state_1
    
    
def test_touch_multiple_objects(db0_fixture):
    obj_1, obj_2 = MemoTestClass(999), MemoTestClass(1000)
    db0.touch(obj_1, obj_2)
    