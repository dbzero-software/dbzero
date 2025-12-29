# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton, MemoScopedSingleton, MemoDataPxSingleton
from .memo_test_types import MemoSingletonWithMigrations
    
from .conftest import DB0_DIR
from dbzero.memo import __dyn_prefix


def test_memo_singleton_is_created_once(db0_fixture):
    object_1 = MemoTestSingleton(999, "text")
    object_2 = MemoTestSingleton()
    assert repr(db0.uuid(object_1)) == repr(db0.uuid(object_2))


def test_memo_singleton_can_be_accessed_with_fetch(db0_fixture):
    object_1 = MemoTestSingleton(999, "text")  
    object_2 = db0.fetch(MemoTestSingleton)
    assert repr(db0.uuid(object_1)) == repr(db0.uuid(object_2))


def test_memo_singleton_can_be_deleted(db0_fixture):
    object_1 = MemoTestSingleton(999, "text")
    db0.delete(object_1)
    with pytest.raises(Exception):
        object_2 = MemoTestSingleton()


def test_singleton_can_be_fetched_by_id(db0_fixture):
    prefix = db0.get_current_prefix()
    object_x = MemoTestClass("x")
    object_1 = MemoTestSingleton(999, object_x)
    id1 = db0.uuid(object_1)
    id2 = db0.uuid(object_x)
    db0.commit()
    db0.close()

    db0.init(DB0_DIR)
    db0.open(prefix.name, "r")
    assert db0.is_singleton(db0.fetch(id1))
    assert not db0.is_singleton(db0.fetch(id2))
    
    
def test_create_then_open_dynamically_scoped_singleton(db0_fixture):
    obj = MemoScopedSingleton(123, prefix="my-temp-prefix-1")
    uuid = db0.uuid(obj)
    db0.close("my-temp-prefix-1")
    # open prefix as read-only
    db0.open("my-temp-prefix-1", "r")
    # open existing singleton
    obj = MemoScopedSingleton(prefix="my-temp-prefix-1")
    assert db0.uuid(obj) == uuid
    assert obj.value == 123
    

def test_find_singleton_static_scope(db0_fixture):
    # find singleton with a static-scope
    assert db0.find_singleton(MemoDataPxSingleton) is None
    obj_1 = MemoDataPxSingleton(789)
    assert db0.find_singleton(MemoDataPxSingleton) is obj_1
    
    
def test_find_singleton(db0_fixture):
    assert db0.find_singleton(MemoTestSingleton) is None    
    # create on default prefix
    obj_1 = MemoScopedSingleton(123)
    assert db0.find_singleton(MemoScopedSingleton) is obj_1
    assert db0.find_singleton(MemoScopedSingleton, "my-test-prefix-1") is None
    # create scoped singleton
    obj_2 = MemoScopedSingleton(456, prefix="my-test-prefix-1")
    assert db0.find_singleton(MemoScopedSingleton, prefix = "my-test-prefix-1") is obj_2


def test_singleton_with_migrations(db0_fixture):
    obj_1 = MemoSingletonWithMigrations(123)
    
    
def test_assembling_dyn_prefix_function(db0_fixture):
    assert __dyn_prefix(MemoTestSingleton) is None
    assert __dyn_prefix(MemoScopedSingleton) is not None
    