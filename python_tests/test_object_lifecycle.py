# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from typing import Any
import pytest
import dbzero as db0
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton, DynamicDataClass


def test_unreferenced_object_is_dropped_on_del_from_python(db0_fixture):
    object_1 = MemoTestClass(123)
    uuid = db0.uuid(object_1)
    del object_1
    db0.commit()
    # object should be dropped from dbzero    
    with pytest.raises(Exception):
        db0.fetch(uuid)


def test_object_cannot_be_deleted_if_references_to_it_exist(db0_fixture):
    object_1 = MemoTestClass(123)
    # object_1 gets referenced from the singleton
    root = MemoTestSingleton(object_1)
    # attempt to delete object_1 should raise an exception
    with pytest.raises(Exception):
        db0.delete(object_1)


def test_unreferenced_object_are_persisted_throughout_series_of_commits(db0_fixture):
    object_1 = MemoTestClass(123)
    db0.commit()    
    # object persisted after commit
    assert object_1.value == 123
    object_1.value = 567
    db0.commit()    
    assert object_1.value == 567


def test_unreferenced_object_are_dropped_on_close(db0_fixture):
    prefix = db0.get_current_prefix()
    object_1 = MemoTestClass(123)
    uuid = db0.uuid(object_1)
    del object_1
    db0.close()
    
    db0.init(DB0_DIR)
    # open as read-write
    db0.open(prefix.name)
    with pytest.raises(Exception):
        db0.fetch(uuid)
    

def test_unreferenced_posvt_member_is_dropped_on_parent_destroy(db0_fixture):
    member = MemoTestClass(123123)
    member_uuid = db0.uuid(member)
    object_1 = MemoTestClass(member)
    del member
    db0.delete(object_1)
    del object_1    
    db0.commit()
    # member object should no longer exist in dbzero
    with pytest.raises(Exception):
        db0.fetch(member_uuid)


def test_unreferenced_indexvt_member_is_dropped_on_parent_destroy(db0_fixture):
    member = MemoTestClass(123123)
    uuid = db0.uuid(member)
    object_1 = DynamicDataClass(120)
    # initialize with index-vt member
    object_2 = DynamicDataClass([87], values = {87: member})
    del member
    db0.delete(object_1)
    db0.delete(object_2)
    del object_1
    del object_2    
    # member object should no longer exist in dbzero
    with pytest.raises(Exception):
        db0.fetch(uuid)


def test_unreferenced_kvindex_member_is_dropped_on_parent_destroy(db0_fixture):
    member = MemoTestClass(123123)
    uuid = db0.uuid(member)
    object_1 = MemoTestClass(123)
    # assign kv-index member
    object_1.kv_member = member
    del member
    db0.delete(object_1)
    del object_1    
    # member object should no longer exist in dbzero
    with pytest.raises(Exception):
        db0.fetch(uuid)


def test_multiple_py_instances_pointing_to_same_unreferenced_object(db0_fixture):
    object_1 = MemoTestClass(123)
    uuid = db0.uuid(object_1)
    object_2 = db0.fetch(uuid)
    del object_1
    # object should be still in db0
    db0.fetch(uuid)
    del object_2
    db0.commit()
    # object should be dropped from dbzero
    with pytest.raises(Exception):
        db0.fetch(uuid)


def test_unreferenced_snapshot_objects_issue_1(db0_fixture):
    """
    This test was failing due to an attempt to drop unreferenced objects from read-only snapshot.
    """
    obj_1 = MemoTestClass(9123)
    db0.commit()
    snap_1 = db0.snapshot()
    # retrieve unreferenced object from snapshot (exists since it had python reference on commit)    
    assert snap_1.fetch(db0.uuid(obj_1)).value == 9123