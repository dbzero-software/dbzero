# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from contextlib import ExitStack
from .memo_test_types import MemoTestClass, MemoDataPxClass, DATA_PX


class SnapshotWindow:
    """
    A context manager that combines two other context managers.
    """
    def __init__(self, from_state, to_state = None, prefix = None):
        if prefix:
            self.pre_context = db0.snapshot({prefix: from_state - 1}) if from_state > 1 else None
            self.last_context = db0.snapshot({prefix: to_state})
        else:
            self.pre_context = db0.snapshot(from_state - 1) if from_state > 1 else None
            self.last_context = db0.snapshot(to_state)
        self._exit_stack = ExitStack()

    def __enter__(self):
        pre_snapshot = self._exit_stack.enter_context(self.pre_context)
        last_snapshot = self._exit_stack.enter_context(self.last_context)
        return pre_snapshot, last_snapshot

    def __exit__(self, exc_type, exc_value, traceback):
        return self._exit_stack.__exit__(exc_type, exc_value, traceback)
    

def test_select_new_pre_bound(db0_fixture, memo_tags):
    db0.commit()
    state_1 = db0.get_state_num(finalized = True)
    obj_x = MemoTestClass(9999)
    db0.tags(obj_x).add("tag1")
    db0.commit()
    state_2 = db0.get_state_num(finalized = True)

    with SnapshotWindow(state_2) as (pre_snap, last_snap):
        assert len(db0.select_new(db0.find(MemoTestClass), pre_snap, last_snap)) == 1

    with SnapshotWindow(state_1) as (pre_snap, last_snap):
        assert len(db0.select_new(db0.find(MemoTestClass), pre_snap, last_snap)) == 11
        
    
def test_select_deleted(db0_fixture, memo_tags):
    db0.commit()
    state_1 = db0.get_state_num(finalized = True)    
    # un-tag single object
    obj_x = next(iter(db0.find(MemoTestClass, "tag1")))
    db0.tags(obj_x).remove("tag1")
    db0.commit()
    state_2 = db0.get_state_num(finalized = True)
    with SnapshotWindow(state_1) as (pre_snap, last_snap):    
        assert len(db0.select_deleted(db0.find(MemoTestClass, "tag1"), pre_snap, last_snap)) == 0
    
    with SnapshotWindow(state_2) as (pre_snap, last_snap):
        assert len(db0.select_deleted(db0.find(MemoTestClass, "tag1"), pre_snap, last_snap)) == 1


def test_select_modified_does_not_include_created(db0_fixture, memo_tags):
    db0.commit()
    state_1 = db0.get_state_num(finalized = True)
    obj_x = MemoTestClass(9999)
    db0.tags(obj_x).add("tag1")
    db0.commit()
    state_2 = db0.get_state_num(finalized = True)
    with SnapshotWindow(state_1) as (pre_snap, last_snap):
        assert len(db0.select_modified(db0.find(MemoTestClass), pre_snap, last_snap)) == 0
    
    with SnapshotWindow(state_2) as (pre_snap, last_snap):
        # NOTE: there may be some number of false positives which we're unable to determine
        assert len(db0.select_modified(db0.find(MemoTestClass), pre_snap, last_snap)) <= 10
        # but for sure, obj_x should not be in the result set
        for _obj in db0.select_modified(db0.find(MemoTestClass), pre_snap, last_snap):
            assert _obj != obj_x


def test_select_modified_without_filter_returns_tuples(db0_fixture, memo_tags):
    db0.commit()
    # modify 1 object in a separate transaction
    obj_1 = next(iter(db0.find(MemoTestClass, "tag1")))
    obj_1.value = 99999
    db0.commit()
    state_2 = db0.get_state_num(finalized = True)
    with SnapshotWindow(state_2) as (pre_snap, last_snap):
        count = 0
        for ver_1, ver_2 in db0.select_modified(db0.find(MemoTestClass), pre_snap, last_snap):
            assert ver_1.value is not None
            assert ver_2.value is not None        
            count += 1
        
        assert count > 0
    
    
def test_select_modified_with_custom_filter(db0_fixture, memo_tags):
    def _compare(obj_1, obj_2):
        return obj_1.value == obj_2.value
    
    db0.commit()
    # modify 1 object in a separate transaction
    obj_1 = next(iter(db0.find(MemoTestClass, "tag1")))
    obj_1.value = 99999
    db0.commit()
    state_2 = db0.get_state_num(finalized = True)
    with SnapshotWindow(state_2) as (pre_snap, last_snap):
        query = db0.select_modified(db0.find(MemoTestClass), pre_snap, last_snap, compare_with = _compare)
        assert len(query) == 1
        object_modification_tuple = next(iter(query))
        assert object_modification_tuple[1].value == 99999


def test_select_modified_can_identify_touched_objects(db0_fixture, memo_tags):
    db0.commit()
    # touch 1 object in a separate transaction
    obj_1 = next(iter(db0.find(MemoTestClass, "tag1")))
    db0.touch(obj_1)
    db0.commit()
    state_2 = db0.get_state_num(finalized = True)
    with SnapshotWindow(state_2) as (pre_snap, last_snap):
        query = db0.select_modified(db0.find(MemoTestClass), pre_snap, last_snap)
        # NOTE: false positives are possible
        assert len(query) > 0
        values = set([x[1].value for x in query])
        assert obj_1.value in values
    
    
def test_select_new_miltiple_prefixes(db0_fixture, memo_tags):
    db0.commit()
    state_1 = db0.get_state_num(finalized = True)
    obj_x = MemoTestClass(9999)
    db0.tags(obj_x).add("tag1")
    db0.commit()
    state_2 = db0.get_state_num(finalized = True)

    with SnapshotWindow(state_1) as (pre_snap, last_snap):
        assert len(db0.select_new(db0.find(MemoTestClass), pre_snap, last_snap)) == 11

    with SnapshotWindow(state_2) as (pre_snap, last_snap):
        assert len(db0.select_new(db0.find(MemoTestClass), pre_snap, last_snap)) == 1    
    obj_x = MemoDataPxClass(9999)
    # NOTE: obj_x exists in the snapshot because it was referenced from Python on commit
    db0.commit()
    
    state_1 = db0.get_state_num(DATA_PX, finalized = True)
    snap_1 = db0.snapshot()
    # NOTE: here obj_x from the prevous snapshot is destroyed (all references are lost)
    obj_x = MemoDataPxClass(9999)
    db0.tags(obj_x).add("tag1")
    db0.commit()
    state_2 = db0.get_state_num(DATA_PX, finalized = True)
    snap_2 = db0.snapshot()
    
    assert len(db0.select_new(db0.find(MemoDataPxClass), snap_1, snap_2)) == 1

    with SnapshotWindow(state_1, state_2, DATA_PX) as (pre_snap, last_snap):
        assert len(db0.select_new(db0.find(MemoDataPxClass), pre_snap, last_snap)) == 1
    

@db0.memo(prefix="some/test/prefix")
class TestClassWithPrefix:
    __test__ = False

    def __init__(self, value):
        self.value = value        


@db0.memo(prefix="/some/test/prefix/x")
class TestClassWithPrefixStartingWithSlash:
    __test__ = False

    def __init__(self, value):
        self.value = value

@pytest.mark.parametrize("ClassType,prefix", [(TestClassWithPrefixStartingWithSlash, "/some/test/prefix/x"),
                                              (TestClassWithPrefix, "some/test/prefix")])
def test_select_with_prefix(db0_fixture, ClassType, prefix):
    obj = ClassType(111)
    db0.tags(obj).add("tag1")
    db0.commit()
    
    obj_x = ClassType(222)
    db0.tags(obj).add("tag1")
    # NOTE: obj_x exists in the snapshot because it was referenced from Python on commit
    db0.commit()
    
    state_1 = db0.get_state_num(prefix, finalized = True)
    snap_1 = db0.snapshot()
    db0.tags(ClassType(333)).add("tag1")
    db0.commit()
    state_2 = db0.get_state_num(prefix, finalized = True)
    snap_2 = db0.snapshot()
    
    assert len(db0.select_new(db0.find(ClassType), snap_1, snap_2)) == 1
    
    # NOTE: SnapshotWindow takes the inclusive range of states
    with SnapshotWindow(state_1, state_2, prefix) as (pre_snap, last_snap):
        assert {x.value for x in db0.select_new(db0.find(ClassType), pre_snap, last_snap)} == {222, 333}
        assert len(db0.select_new(db0.find(ClassType), pre_snap, last_snap)) == 2
    
    
@db0.memo(prefix="test_cud_find_new")
class ValueWrapper:
    def __init__(self, value):
        self.value = value

def test_select_new_handles_new_prefixes(db0_fixture):
    snap_1 = db0.snapshot()
    db0.commit()

    # NOTE: a new prefix is created here
    value_1 = ValueWrapper("asd")
    db0.tags(value_1).add("some tag")
    db0.commit()
    snap_2 = db0.snapshot()
    results = db0.select_new(db0.find(ValueWrapper), snap_1, snap_2)
    assert len(results) == 1


def test_select_new_results_are_stable(db0_fixture):    
    db0.commit()
    snap_1 = db0.snapshot()    
    db0.tags(ValueWrapper("asd")).add("some tag")
    db0.commit()
    snap_2 = db0.snapshot()
    db0.tags(ValueWrapper("qwe")).add("some tag")
    db0.commit()

    # make sure results are not affected by additional commits
    results = db0.select_new(db0.find(ValueWrapper), snap_1, snap_2)
    assert len(results) == 1


def test_select_new_other_prefix(db0_fixture):
    snap_0 = db0.snapshot()
    db0.commit()
    snap_1 = db0.snapshot()

    value_1 = ValueWrapper("asd")
    db0.tags(value_1).add("some tag")
    db0.commit()
    snap_2 = db0.snapshot()
    results = db0.select_new(db0.find(ValueWrapper), snap_0, snap_2)
    assert len(results) == 1
    results = db0.select_new(db0.find(ValueWrapper), snap_1, snap_2)
    assert len(results) == 1

    value_2 = ValueWrapper("qwe")
    db0.tags(value_2).add("some tag")
    db0.commit()
    snap_3 = db0.snapshot()

    results = db0.select_new(db0.find(ValueWrapper), snap_1, snap_2)
    assert len(results) == 1

    results = db0.select_new(db0.find(ValueWrapper), snap_1, snap_3)
    assert len(results) == 2