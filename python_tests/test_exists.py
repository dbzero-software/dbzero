# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, DynamicDataClass, MemoDataPxSingleton, MemoTestSingleton


def test_memo_object_exists_by_uuid(db0_fixture):
    object_1 = MemoTestClass(123)
    assert db0.exists(db0.uuid(object_1))


def test_exists_with_type_validation(db0_fixture):
    object_1 = MemoTestClass(123)
    assert not db0.exists(db0.uuid(object_1), DynamicDataClass)


def test_scoped_singleton_exists(db0_fixture):
    obj_1 = MemoDataPxSingleton(123)
    assert db0.exists(db0.uuid(obj_1))    
    assert db0.exists(MemoDataPxSingleton)
    
    
def test_exists_singleton_with_prefix_specification(db0_fixture):
    px1 = db0.get_current_prefix().name
    _ = MemoTestSingleton(123)
    db0.open("my-other-prefix", "rw")
    _ = MemoTestSingleton(456)
    assert db0.exists(MemoTestSingleton, prefix=px1)
    assert db0.exists(MemoTestSingleton, prefix="my-other-prefix")


def test_exists_after_deletion(db0_fixture):
    obj_1 = MemoTestClass(123)
    uuid_1 = db0.uuid(obj_1)
    db0.delete(obj_1)
    del obj_1    
    db0.commit()
    assert not db0.exists(uuid_1)
        

def test_exists_with_corrupt_uuid(db0_fixture):
    obj_1 = MemoTestClass(123)
    uuid_1 = db0.uuid(obj_1)
    assert db0.exists(uuid_1)
    assert not db0.exists(uuid_1[:-1])
    assert not db0.exists(uuid_1 + "XAX")
