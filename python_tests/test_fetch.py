# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, DynamicDataClass, MemoDataPxSingleton, MemoTestSingleton


def test_memo_object_can_be_fetched_by_id(db0_fixture):
    object_1 = MemoTestClass(123)
    object_2 = db0.fetch(db0.uuid(object_1))
    assert object_2 is not None
    assert object_2.value == 123


def test_fetch_can_validate_memo_type(db0_fixture):
    object_1 = MemoTestClass(123)
    # fetching by incorrect type should raise
    with pytest.raises(Exception):
        db0.fetch(db0.uuid(object_1), DynamicDataClass)
    object_2 = db0.fetch(db0.uuid(object_1), MemoTestClass)
    assert object_2 is not None
    assert object_2.value == 123


def test_fetch_scoped_singleton(db0_fixture):
    obj_1 = MemoDataPxSingleton(123)
    assert db0.fetch(db0.uuid(obj_1)) == obj_1
    # fetch by type only
    assert db0.fetch(MemoDataPxSingleton) == obj_1
    
    
def test_fetch_singleton_from_specific_prefix(db0_fixture):
    px1 = db0.get_current_prefix().name
    _ = MemoTestSingleton(123)
    db0.open("my-other-prefix", "rw")
    _ = MemoTestSingleton(456)
    assert db0.fetch(MemoTestSingleton, prefix=px1).value == 123
    assert db0.fetch(MemoTestSingleton, prefix="my-other-prefix").value == 456    
